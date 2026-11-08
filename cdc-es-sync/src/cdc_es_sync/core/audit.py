"""Static checks of the field map against the document SQL. Each returns problem texts; empty means consistent.

Run by the `check` command, at start-up, and offline by the unit tests (with tests/fixtures/table_columns.json).
"""
from __future__ import annotations

from collections import defaultdict

from .model import REBUILD
from .registry import Registry
from .sqlscan import column_refs


def unmapped_tables(registry: Registry) -> list[str]:
    """Every table a document query reads must have a source entry or be listed under `not_propagated`."""
    problems = []
    for name, spec in registry.indices.items():
        mapped = set(registry.rules_of(name))
        loose = registry.tables_in_sql(name) - mapped - set(spec.not_propagated)
        if loose:
            problems.append(f"{name}: tables in the SQL with no mapping and no exclusion: {sorted(loose)}")
    return problems


def audit_field_map(registry: Registry, table_columns: dict[str, set[str]]) -> list[str]:
    """Checks the field map against what each document query really reads.

    table_columns: TABLE -> set of COLUMN names (upper case), for the tables the queries read.
    """
    problems = []
    for name, spec in registry.indices.items():
        refs = column_refs(spec.select, table_columns)
        derived: dict[tuple[str, str], set[str]] = defaultdict(set)     # (TABLE, COLUMN) -> fields it feeds
        for fld, pairs in refs.fields.items():
            for pair in pairs:
                derived[pair].add(fld)
        routes = registry.rules_of(name)
        read = {t for t, _ in derived} | {t for t, _ in refs.join} | set(refs.top_tables)
        excluded = set(spec.not_propagated)

        for table in sorted(read - set(routes) - excluded):
            problems.append(f"{name}: query reads {table} but no source maps it (and it is not excluded)")
        for table in sorted(set(routes) - read):
            problems.append(f"{name}: {table} is mapped but the query does not read it")
        for table in sorted(set(refs.top_tables) - excluded):
            if table in routes and not any(r.join for r in routes[table]):
                problems.append(f"{name}: {table} is in the query's FROM list but no source says join: inner|outer")

        for table, rules in routes.items():
            configured: dict[str, set[str]] = defaultdict(set)
            rebuilt: set[str] = set()
            for rule in rules:
                for column, col_spec in rule.columns.items():
                    pair = (table, column)
                    if pair not in derived and pair not in refs.join:
                        problems.append(f"{rule.id}: column {column} is not read by the query")
                        continue
                    if col_spec == REBUILD:
                        rebuilt.add(column)
                        continue
                    configured[column].update(col_spec)
                    extra = set(col_spec) - derived.get(pair, set())
                    if extra:
                        problems.append(f"{rule.id}: {column} does not feed {sorted(extra)} in the query")
            for (t, column), fields in sorted(derived.items()):
                if t != table or column in rebuilt:
                    continue
                missing = fields - configured.get(column, set())
                if missing:
                    problems.append(f"{name}: {table}.{column} feeds {sorted(missing)} but no source refreshes them")
            for t, column in sorted(refs.join):
                if t != table:
                    continue
                for rule in rules:
                    if rule.join and rule.columns.get(column) != REBUILD:
                        problems.append(f"{rule.id}: join column {column} must be 'rebuild'")
    return problems
