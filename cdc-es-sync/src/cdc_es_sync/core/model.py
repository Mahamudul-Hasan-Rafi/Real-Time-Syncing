"""The compiled field map: what an index is, what one source rule says, and what a batch of events asks for.

Vocabulary (the same words as in the index files):

    IndexSpec    one Elasticsearch index: its document query, key, id format and conversions
    SourceRule   one entry under `sources.<TABLE>`: how a change of that table finds the affected documents
                 (`direct` key columns or a `sql` lookup) and which document fields each changed column feeds
    ChangePlan   what a batch of events asks for: documents to rebuild whole, fields to refresh, lookups to run
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .events import Change
from .keys import column_changed, normalize_key
from .sqlscan import Select

REBUILD = "rebuild"      # column spec: a key or join column, so the whole document is rebuilt
FULL = "full"            # decision: rebuild the whole document

Key = tuple              # one document key: the normalized values of the index's key columns, in order


@dataclass
class IndexSpec:
    name: str
    target: str
    es_index: str
    sql: str
    select: Select
    key_columns: list[tuple[str, str]]          # [(output column of the query, key type)]
    key_filter: list[str]                       # the SQL expression of each key column, for WHERE clauses
    doc_id: str                                 # format string over the key columns
    timestamp_field: str | None
    json_columns: list[str]
    nested_date_fields: dict[str, list[str]]
    etl_pipeline: str
    not_propagated: dict[str, Any] = field(default_factory=dict)
    reconcile_sum_field: str | None = None      # numeric output field totalled by the `reconcile` command

    @property
    def key_types(self) -> list[str]:
        return [t for _, t in self.key_columns]

    @property
    def key_names(self) -> list[str]:
        return [c for c, _ in self.key_columns]

    @property
    def fields(self) -> list[str]:
        return self.select.names

    def key_from_row(self, row: dict[str, Any]) -> Key | None:
        values = tuple(normalize_key(row.get(col), typ) for col, typ in self.key_columns)
        return None if any(v is None for v in values) else values


@dataclass
class SourceRule:
    id: str                                     # "<index>.<TABLE>#<n>", used in logs and audit messages
    index: str
    direct: list[str] | None = None             # event columns forming the document key
    sql: str | None = None                      # lookup query returning document keys
    params: dict[str, str] = field(default_factory=dict)       # bind name -> key type
    join: str | None = None                     # "inner" / "outer": this table is in the query's FROM list
    columns: dict[str, Any] = field(default_factory=dict)      # COLUMN -> REBUILD | tuple of document fields

    def event_columns(self) -> list[str]:
        """The event columns this rule needs to locate documents (they must be in a supplemental log group)."""
        return list(self.direct) if self.direct else list(self.params)

    @property
    def all_fields(self) -> set[str]:
        out: set[str] = set()
        for spec in self.columns.values():
            if spec != REBUILD:
                out.update(spec)
        return out

    def decide(self, change: Change) -> str | set[str] | None:
        """None: nothing to do. FULL: rebuild the document. Otherwise the set of fields to refresh."""
        if change.op != "u":
            if self.join == "inner":
                return FULL                 # the row's existence decides whether the document exists
            return self.all_fields or FULL
        before, after = change.before or {}, change.after or {}
        fields: set[str] = set()
        for column, spec in self.columns.items():
            if column_changed(before.get(column), after.get(column)):
                if spec == REBUILD:
                    return FULL
                fields.update(spec)
        return fields or None


@dataclass
class PendingLookup:
    """A lookup query to run once per batch for one set of bind values."""

    rule: SourceRule
    fields: set[str] | None             # None: rebuild the documents it returns whole


@dataclass
class ChangePlan:
    rebuild: dict[str, set[Key]] = field(default_factory=dict)                  # index -> keys to rebuild whole
    partial: dict[str, dict[Key, set[str]]] = field(default_factory=dict)       # index -> {key: fields to refresh}
    lookups: dict[tuple, PendingLookup] = field(default_factory=dict)           # (rule id, bind values) -> lookup
    unchanged: int = 0                  # rules skipped because none of their columns changed

    def add_direct(self, index: str, key: Key, decision: str | set[str]) -> None:
        if decision == FULL:
            self.rebuild.setdefault(index, set()).add(key)
        else:
            self.partial.setdefault(index, {}).setdefault(key, set()).update(decision)

    def add_lookup(self, slot: tuple, rule: SourceRule, decision: str | set[str]) -> None:
        pending = self.lookups.get(slot)
        if pending is None:
            self.lookups[slot] = PendingLookup(rule, None if decision == FULL else set(decision))
        elif pending.fields is not None:
            if decision == FULL:
                pending.fields = None
            else:
                pending.fields.update(decision)
