"""Which documents, and which fields of them, a change event affects. Built from the `indices` config.

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
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path

from .events import Change
from .sqlscan import Select, column_refs, parse_select

_EPOCH = datetime(1970, 1, 1)
_TYPES = ("int", "str", "date")
REBUILD = "rebuild"
FULL = "full"            # decision: rebuild the whole document


class MappingError(Exception):
    """The configuration is inconsistent."""


class MissingKeyError(Exception):
    """An event does not carry the columns the mapping needs, usually a missing supplemental log group."""


def norm(value, typ: str):
    """One canonical Python value per key type, whether it came from Debezium or from Oracle."""
    if value is None:
        return None
    if typ == "int":
        if isinstance(value, bool):
            raise ValueError("boolean is not a key")
        if isinstance(value, int):
            return value
        try:
            number = Decimal(str(value).strip())
        except InvalidOperation as exc:
            raise ValueError(f"not a number: {value!r}") from exc
        if number != number.to_integral_value():
            raise ValueError(f"not a whole number: {value!r}")
        return int(number)
    if typ == "str":
        return str(value)
    if typ == "date":
        if isinstance(value, datetime):
            return value.replace(microsecond=0, tzinfo=None)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return _EPOCH + timedelta(milliseconds=value)       # Debezium: wall clock as epoch millis
        text = str(value).strip()
        if re.fullmatch(r"-?\d+", text):
            return _EPOCH + timedelta(milliseconds=int(text))
        return datetime.fromisoformat(text.replace("Z", "")).replace(microsecond=0, tzinfo=None)
    raise ValueError(f"unknown key type {typ!r}")


def differs(before, after) -> bool:
    """Did a column change? Oracle cannot tell an empty string from NULL, and neither can we."""
    if before == "":
        before = None
    if after == "":
        after = None
    return before != after


@dataclass
class IndexCfg:
    name: str
    target: str
    es_index: str
    sql: str
    select: Select
    key_cols: list              # [(row column, type)]
    key_filter: list
    doc_id: str
    timestamp_field: str | None
    json_columns: list
    nested_date_fields: dict
    etl_pipeline: str
    not_propagated: dict = field(default_factory=dict)
    reconcile_sum_field: str | None = None      # numeric output field totalled by the `reconcile` command

    @property
    def key_types(self):
        return [t for _, t in self.key_cols]

    @property
    def fields(self) -> list:
        return self.select.names

    def key_from_row(self, row: dict):
        values = tuple(norm(row.get(col), typ) for col, typ in self.key_cols)
        return None if any(v is None for v in values) else values


@dataclass
class Impact:
    id: str
    index: str
    direct: list | None = None      # event columns forming the key
    sql: str | None = None
    params: dict = field(default_factory=dict)      # bind name -> type
    join: str | None = None         # "inner" / "outer": this entry is the table the query joins on
    columns: dict = field(default_factory=dict)     # COLUMN -> REBUILD | tuple of document fields

    def event_columns(self):
        return list(self.direct) if self.direct else list(self.params)

    @property
    def all_fields(self) -> set:
        out = set()
        for spec in self.columns.values():
            if spec != REBUILD:
                out.update(spec)
        return out

    def decide(self, change: Change):
        """None: nothing to do. FULL: rebuild the document. Otherwise the set of fields to refresh."""
        if change.op != "u":
            if self.join == "inner":
                return FULL                 # the row's existence decides whether the document exists
            return self.all_fields or FULL
        before, after = change.before or {}, change.after or {}
        fields: set = set()
        for column, spec in self.columns.items():
            if differs(before.get(column), after.get(column)):
                if spec == REBUILD:
                    return FULL
                fields.update(spec)
        return fields or None


@dataclass
class Plan:
    full: dict = field(default_factory=dict)        # index -> set of keys to rebuild
    part: dict = field(default_factory=dict)        # index -> {key: set of fields}
    lookups: dict = field(default_factory=dict)     # (impact id, params tuple) -> [Impact, fields or None (= rebuild)]
    unchanged: int = 0                              # entries skipped because no relevant column changed


class Registry:
    def __init__(self, indices: dict, tables: dict):
        self.indices = indices
        self.tables = tables            # TABLE -> [Impact] across all indices

    @classmethod
    def from_config(cls, cfg: dict) -> "Registry":
        base = Path(cfg["_base"])
        indices, tables = {}, {}
        for name, c in cfg["indices"].items():
            cols = [(k["col"].lower(), k["type"]) for k in c["key_columns"]]
            for _, typ in cols:
                if typ not in _TYPES:
                    raise MappingError(f"{name}: unknown key type {typ}")
            if len(c["key_filter"]) != len(cols):
                raise MappingError(f"{name}: key_filter and key_columns differ in length")
            sql = (base / c["sql_file"]).read_text(encoding="utf-8").strip().rstrip(";")
            select = parse_select(sql)
            for col, _ in cols:
                if col not in select.names:
                    raise MappingError(f"{name}: key column {col} is not an output column of the query")
            indices[name] = IndexCfg(
                name=name, target=c.get("target", "default"), es_index=c["es_index"], sql=sql, select=select,
                key_cols=cols, key_filter=list(c["key_filter"]), doc_id=c["doc_id"],
                timestamp_field=c.get("timestamp_field"), json_columns=list(c.get("json_columns") or []),
                nested_date_fields=dict(c.get("nested_date_fields") or {}),
                etl_pipeline=c.get("etl_pipeline", name),
                not_propagated={k.upper(): v for k, v in (c.get("not_propagated") or {}).items()},
                reconcile_sum_field=c.get("reconcile_sum_field"),
            )
            if indices[name].reconcile_sum_field and indices[name].reconcile_sum_field not in select.names:
                raise MappingError(f"{name}: reconcile_sum_field {c['reconcile_sum_field']} is not an output column")
        for name, c in cfg["indices"].items():
            ix = indices[name]
            for table, entries in (c.get("sources") or {}).items():
                table = table.upper()
                for n, e in enumerate(entries):
                    impact = cls._impact(f"{name}.{table}#{n}", ix, e)
                    tables.setdefault(table, []).append(impact)
        return cls(indices, tables)

    @staticmethod
    def _impact(impact_id: str, ix: IndexCfg, e: dict) -> Impact:
        impact = Impact(id=impact_id, index=ix.name, join=e.get("join"))
        if impact.join not in (None, "inner", "outer"):
            raise MappingError(f"{impact_id}: join must be inner or outer")
        if "direct" in e:
            impact.direct = [c.upper() for c in e["direct"]]
            if len(impact.direct) != len(ix.key_cols):
                raise MappingError(f"{impact_id}: direct needs {len(ix.key_cols)} columns")
        else:
            impact.sql = " ".join(str(e["sql"]).split())
            impact.params = {k.upper(): v for k, v in e["params"].items()}
            for pname, typ in impact.params.items():
                if typ not in _TYPES:
                    raise MappingError(f"{impact_id}: unknown param type {typ}")
                if f":{pname}" not in impact.sql:
                    raise MappingError(f"{impact_id}: bind :{pname} is not used in the SQL")
            used = set(re.findall(r":([A-Za-z_]\w*)", re.sub(r"'[^']*'", "''", impact.sql)))
            if used != set(impact.params):
                raise MappingError(f"{impact_id}: SQL binds {sorted(used)} differ from params {sorted(impact.params)}")
        known = set(ix.fields)
        for column, spec in (e.get("columns") or {}).items():
            column = column.upper()
            if spec == REBUILD:
                impact.columns[column] = REBUILD
                continue
            names = [column.lower()] if spec == "same" else spec
            if not isinstance(names, list) or not names:
                raise MappingError(f"{impact_id}: {column} must be 'rebuild', 'same' or a list of fields")
            unknown = [f for f in names if f not in known]
            if unknown:
                raise MappingError(f"{impact_id}: {column} feeds {unknown}, which {ix.name} does not have")
            impact.columns[column] = tuple(names)
        if not impact.columns:
            raise MappingError(f"{impact_id}: no columns listed")
        for column in impact.event_columns():
            if column not in impact.columns:
                raise MappingError(f"{impact_id}: key/bind column {column} must also be listed under columns")
        if impact.direct:
            for column in impact.direct:
                if impact.columns[column] != REBUILD:
                    raise MappingError(f"{impact_id}: document key column {column} must be 'rebuild'")
        return impact

    # ------------------------------------------------------------------------------------------
    def route(self, change: Change, plan: Plan) -> bool:
        """Adds what one event affects to the plan. False when the table is not mapped at all."""
        impacts = self.tables.get(change.table)
        if not impacts:
            return False
        images = [i for i in (change.before, change.after) if i is not None]
        for impact in impacts:
            decision = impact.decide(change)
            if decision is None:
                plan.unchanged += 1
                continue
            ix = self.indices[impact.index]
            found = False
            for image in images:
                try:
                    if impact.direct:
                        values = tuple(norm(image.get(c), t) for c, t in zip(impact.direct, ix.key_types))
                        ok = all(v is not None for v in values)
                        if ok:
                            self._add_direct(plan, impact.index, values, decision)
                    else:
                        values = tuple((n, norm(image.get(n), t)) for n, t in sorted(impact.params.items()))
                        ok = all(v is not None for _, v in values)
                        if ok:
                            self._add_lookup(plan, (impact.id, values), impact, decision)
                except ValueError as exc:
                    raise MissingKeyError(f"{impact.id}: bad value in {change.table}: {exc}") from exc
                found = found or ok
            if not found:
                raise MissingKeyError(
                    f"{impact.id}: event on {change.table} lacks {impact.event_columns()}; "
                    "check the supplemental log group")
        return True

    @staticmethod
    def _add_direct(plan: Plan, index: str, key: tuple, decision):
        if decision == FULL:
            plan.full.setdefault(index, set()).add(key)
        else:
            plan.part.setdefault(index, {}).setdefault(key, set()).update(decision)

    @staticmethod
    def _add_lookup(plan: Plan, slot: tuple, impact: Impact, decision):
        entry = plan.lookups.get(slot)
        if entry is None:
            plan.lookups[slot] = [impact, None if decision == FULL else set(decision)]
        elif entry[1] is not None:
            if decision == FULL:
                entry[1] = None
            else:
                entry[1].update(decision)

    # ------------------------------------------------------------------------------------------
    def required_columns(self) -> dict:
        """table -> event columns the mapping reads to find the documents (must be in a supplemental log group)."""
        need = {}
        for table, impacts in self.tables.items():
            for impact in impacts:
                need.setdefault(table, set()).update(impact.event_columns())
        return need

    def tables_in_sql(self, index: str) -> set:
        ix = self.indices[index]
        return {m.upper() for m in re.findall(r"(?i)\bemob\.(\w+)", ix.sql)}

    # ------------------------------------------------------------------------------------------
    def audit(self, table_columns: dict) -> list:
        """Checks the field map against what each document query really reads. Returns problem texts.

        table_columns: TABLE -> set of COLUMN names (upper case), for the tables the queries read.
        """
        problems = []
        for name, ix in self.indices.items():
            refs = column_refs(ix.select, table_columns)
            derived = defaultdict(set)              # (TABLE, COLUMN) -> fields it feeds
            for fld, pairs in refs.fields.items():
                for pair in pairs:
                    derived[pair].add(fld)
            routes = defaultdict(list)              # TABLE -> entries of this index
            for table, impacts in self.tables.items():
                routes[table] = [i for i in impacts if i.index == name]
            routes = {t: r for t, r in routes.items() if r}
            read = {t for t, _ in derived} | {t for t, _ in refs.join} | set(refs.top_tables)
            excluded = set(ix.not_propagated)

            for table in sorted(read - set(routes) - excluded):
                problems.append(f"{name}: query reads {table} but no source maps it (and it is not excluded)")
            for table in sorted(set(routes) - read):
                problems.append(f"{name}: {table} is mapped but the query does not read it")
            for table in sorted(set(refs.top_tables) - excluded):
                if table in routes and not any(r.join for r in routes[table]):
                    problems.append(f"{name}: {table} is in the query's FROM list but no source says join: inner|outer")

            for table, entries in routes.items():
                configured, rebuilt = defaultdict(set), set()
                for entry in entries:
                    for column, spec in entry.columns.items():
                        pair = (table, column)
                        if pair not in derived and pair not in refs.join:
                            problems.append(f"{entry.id}: column {column} is not read by the query")
                            continue
                        if spec == REBUILD:
                            rebuilt.add(column)
                            continue
                        configured[column].update(spec)
                        extra = set(spec) - derived.get(pair, set())
                        if extra:
                            problems.append(f"{entry.id}: {column} does not feed {sorted(extra)} in the query")
                for (t, column), fields in sorted(derived.items()):
                    if t != table or column in rebuilt:
                        continue
                    missing = fields - configured.get(column, set())
                    if missing:
                        problems.append(f"{name}: {table}.{column} feeds {sorted(missing)} but no source refreshes them")
                for t, column in sorted(refs.join):
                    if t != table:
                        continue
                    for entry in entries:
                        if entry.join and entry.columns.get(column) != REBUILD:
                            problems.append(f"{entry.id}: join column {column} must be 'rebuild'")
        return problems
