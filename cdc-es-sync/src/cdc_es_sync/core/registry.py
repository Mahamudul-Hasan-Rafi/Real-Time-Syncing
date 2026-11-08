"""Compiles the index files into a Registry, and routes each change event to the documents and fields it affects.

Each index lists its source tables under `sources`. A source entry says how to find the affected documents
(`direct` key columns, or a `sql` lookup with `params`) and which table columns feed which document fields:

    columns:
      MOBILE_NO: same            # column feeds the field of the same name
      OCCUPATION_CODE: [occupation_code, occupation]
      CUST_NO: rebuild           # join / key column: the whole document is rebuilt

An update event is compared before/after. Only entries whose changed columns are listed here do anything,
and only the fields those columns feed are refreshed.
"""
from __future__ import annotations

import re
from typing import Any

from ..errors import MappingError, MissingKeyError
from ..settings import IndexDefinition, Settings
from .events import Change
from .keys import KEY_TYPES, normalize_key
from .model import REBUILD, ChangePlan, IndexSpec, SourceRule
from .sqlscan import parse_select


class Registry:
    def __init__(self, indices: dict[str, IndexSpec], rules_by_table: dict[str, list[SourceRule]],
                 schema: str = "EMOB"):
        self.indices = indices
        self.rules_by_table = rules_by_table        # TABLE -> [SourceRule] across all indices
        self.schema = schema

    @classmethod
    def from_settings(cls, settings: Settings) -> Registry:
        return cls.from_definitions(settings.indices, schema=settings.oracle.schema)

    @classmethod
    def from_definitions(cls, definitions: tuple[IndexDefinition, ...] | list[IndexDefinition],
                         schema: str = "EMOB") -> Registry:
        """Raises MappingError when a definition is inconsistent with itself or with its SQL."""
        indices = {d.name: _compile_index(d) for d in definitions}
        rules_by_table: dict[str, list[SourceRule]] = {}
        for d in definitions:
            spec = indices[d.name]
            for table, entries in (d.spec.get("sources") or {}).items():
                table = table.upper()
                for n, entry in enumerate(entries):
                    rule = _compile_rule(f"{d.name}.{table}#{n}", spec, entry)
                    rules_by_table.setdefault(table, []).append(rule)
        return cls(indices, rules_by_table, schema)

    # -- routing ------------------------------------------------------------------------------------------------
    def route(self, change: Change, plan: ChangePlan) -> bool:
        """Adds what one event affects to the plan. False when the table is not mapped at all."""
        rules = self.rules_by_table.get(change.table)
        if not rules:
            return False
        images = [i for i in (change.before, change.after) if i is not None]
        for rule in rules:
            decision = rule.decide(change)
            if decision is None:
                plan.unchanged += 1
                continue
            spec = self.indices[rule.index]
            found = False
            for image in images:
                try:
                    if rule.direct:
                        key = tuple(normalize_key(image.get(c), t) for c, t in zip(rule.direct, spec.key_types))
                        ok = all(v is not None for v in key)
                        if ok:
                            plan.add_direct(rule.index, key, decision)
                    else:
                        binds = tuple((n, normalize_key(image.get(n), t)) for n, t in sorted(rule.params.items()))
                        ok = all(v is not None for _, v in binds)
                        if ok:
                            plan.add_lookup((rule.id, binds), rule, decision)
                except ValueError as exc:
                    raise MissingKeyError(f"{rule.id}: bad value in {change.table}: {exc}") from exc
                found = found or ok
            if not found:
                raise MissingKeyError(
                    f"{rule.id}: event on {change.table} lacks {rule.event_columns()}; "
                    "check the supplemental log group")
        return True

    # -- facts the start-up checks need -------------------------------------------------------------------------
    def required_columns(self) -> dict[str, set[str]]:
        """table -> event columns the mapping reads to find the documents (must be in a supplemental log group)."""
        need: dict[str, set[str]] = {}
        for table, rules in self.rules_by_table.items():
            for rule in rules:
                need.setdefault(table, set()).update(rule.event_columns())
        return need

    def tables_in_sql(self, index: str) -> set[str]:
        """Tables of the schema that an index's document query reads."""
        return {m.upper() for m in re.findall(rf"(?i)\b{re.escape(self.schema)}\.(\w+)", self.indices[index].sql)}

    def rules_of(self, index: str) -> dict[str, list[SourceRule]]:
        """TABLE -> the rules of one index."""
        out: dict[str, list[SourceRule]] = {}
        for table, rules in self.rules_by_table.items():
            mine = [r for r in rules if r.index == index]
            if mine:
                out[table] = mine
        return out


# -- compiling the index files ------------------------------------------------------------------------------------
def _compile_index(definition: IndexDefinition) -> IndexSpec:
    name, c = definition.name, definition.spec
    for required in ("es_index", "sql_file", "key_columns", "key_filter", "doc_id"):
        if required not in c:
            raise MappingError(f"{name}: {required} is required")
    cols = [(k["col"].lower(), k["type"]) for k in c["key_columns"]]
    for _, typ in cols:
        if typ not in KEY_TYPES:
            raise MappingError(f"{name}: unknown key type {typ}")
    if len(c["key_filter"]) != len(cols):
        raise MappingError(f"{name}: key_filter and key_columns differ in length")
    sql_path = definition.path.parent / c["sql_file"]
    if not sql_path.is_file():
        raise MappingError(f"{name}: sql_file not found: {sql_path}")
    sql = sql_path.read_text(encoding="utf-8").strip().rstrip(";")
    select = parse_select(sql)
    for col, _ in cols:
        if col not in select.names:
            raise MappingError(f"{name}: key column {col} is not an output column of the query")
    spec = IndexSpec(
        name=name, target=c.get("target", "default"), es_index=c["es_index"], sql=sql, select=select,
        key_columns=cols, key_filter=list(c["key_filter"]), doc_id=c["doc_id"],
        timestamp_field=c.get("timestamp_field"), json_columns=list(c.get("json_columns") or []),
        nested_date_fields=dict(c.get("nested_date_fields") or {}),
        etl_pipeline=c.get("etl_pipeline", name),
        not_propagated={k.upper(): v for k, v in (c.get("not_propagated") or {}).items()},
        reconcile_sum_field=c.get("reconcile_sum_field"),
    )
    if spec.reconcile_sum_field and spec.reconcile_sum_field not in select.names:
        raise MappingError(f"{name}: reconcile_sum_field {spec.reconcile_sum_field} is not an output column")
    return spec


def _compile_rule(rule_id: str, spec: IndexSpec, e: dict[str, Any]) -> SourceRule:
    rule = SourceRule(id=rule_id, index=spec.name, join=e.get("join"))
    if rule.join not in (None, "inner", "outer"):
        raise MappingError(f"{rule_id}: join must be inner or outer")
    if "direct" in e:
        rule.direct = [c.upper() for c in e["direct"]]
        if len(rule.direct) != len(spec.key_columns):
            raise MappingError(f"{rule_id}: direct needs {len(spec.key_columns)} columns")
    else:
        if "sql" not in e or "params" not in e:
            raise MappingError(f"{rule_id}: give either direct, or sql with params")
        rule.sql = " ".join(str(e["sql"]).split())
        rule.params = {k.upper(): v for k, v in e["params"].items()}
        for pname, typ in rule.params.items():
            if typ not in KEY_TYPES:
                raise MappingError(f"{rule_id}: unknown param type {typ}")
            if f":{pname}" not in rule.sql:
                raise MappingError(f"{rule_id}: bind :{pname} is not used in the SQL")
        used = set(re.findall(r":([A-Za-z_]\w*)", re.sub(r"'[^']*'", "''", rule.sql)))
        if used != set(rule.params):
            raise MappingError(f"{rule_id}: SQL binds {sorted(used)} differ from params {sorted(rule.params)}")
    known = set(spec.fields)
    for column, col_spec in (e.get("columns") or {}).items():
        column = column.upper()
        if col_spec == REBUILD:
            rule.columns[column] = REBUILD
            continue
        names = [column.lower()] if col_spec == "same" else col_spec
        if not isinstance(names, list) or not names:
            raise MappingError(f"{rule_id}: {column} must be 'rebuild', 'same' or a list of fields")
        unknown = [f for f in names if f not in known]
        if unknown:
            raise MappingError(f"{rule_id}: {column} feeds {unknown}, which {spec.name} does not have")
        rule.columns[column] = tuple(names)
    if not rule.columns:
        raise MappingError(f"{rule_id}: no columns listed")
    for column in rule.event_columns():
        if column not in rule.columns:
            raise MappingError(f"{rule_id}: key/bind column {column} must also be listed under columns")
    if rule.direct:
        for column in rule.direct:
            if rule.columns[column] != REBUILD:
                raise MappingError(f"{rule_id}: document key column {column} must be 'rebuild'")
    return rule
