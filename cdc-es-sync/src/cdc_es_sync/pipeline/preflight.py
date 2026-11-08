"""Start-up checks, also run by `python -m cdc_es_sync check`. Each proves one precondition against the live systems."""
from __future__ import annotations

from collections.abc import Callable
from typing import NamedTuple

from ..connectors.elastic import ElasticsearchWriter
from ..connectors.kafka import topic_count
from ..connectors.oracle import OracleReader
from ..core.audit import audit_field_map, unmapped_tables
from ..core.registry import Registry
from ..settings import Settings

SUPPLEMENTAL_LOGGING = "supplemental logging"      # the one check that can be made non-fatal


class CheckResult(NamedTuple):
    name: str
    ok: bool
    detail: str


class Preflight:
    def __init__(self, settings: Settings, registry: Registry, reader: OracleReader, writer: ElasticsearchWriter):
        self.settings = settings
        self.registry = registry
        self.reader = reader
        self.writer = writer

    def run(self) -> list[CheckResult]:
        """Runs every check, in order; a failure does not stop the others."""
        checks: list[tuple[str, Callable[[], str]]] = [
            ("kafka reachable", self.kafka_reachable),
            ("elasticsearch reachable", self.elasticsearch_reachable),
            ("elasticsearch indices", self.elasticsearch_indices),
            ("oracle reachable", self.oracle_reachable),
            ("document queries", self.document_queries),
            ("lookup queries", self.lookup_queries),
            (SUPPLEMENTAL_LOGGING, self.supplemental_logging),
            ("mapping coverage", self.mapping_coverage),
            ("field map", self.field_map),
        ]
        results = []
        for name, check in checks:
            try:
                results.append(CheckResult(name, True, check() or "ok"))
            except Exception as exc:
                results.append(CheckResult(name, False, f"{type(exc).__name__}: {exc}"))
        return results

    def is_fatal(self, result: CheckResult) -> bool:
        if result.ok:
            return False
        return result.name != SUPPLEMENTAL_LOGGING or self.settings.startup_checks.fail_on_missing_logging

    # -- the checks ---------------------------------------------------------------------------------------------
    def kafka_reachable(self) -> str:
        return f"{topic_count(self.settings.kafka)} topics"

    def elasticsearch_reachable(self) -> str:
        self.writer.ping_all()
        return "all targets answer"

    def elasticsearch_indices(self) -> str:
        missing = [spec.es_index for spec in self.registry.indices.values()
                   if not self.writer.index_exists(spec.target, spec.es_index)]
        if missing:
            raise RuntimeError(f"index missing: {missing}")
        return "all indices exist"

    def oracle_reachable(self) -> str:
        self.reader.ping()
        return "ok"

    def document_queries(self) -> str:
        for spec in self.registry.indices.values():
            names = self.reader.output_names(f"{spec.sql}\n AND 1 = 0")
            if set(names) != set(spec.fields):
                raise RuntimeError(f"{spec.name}: parsed output columns differ from Oracle's: "
                                   f"{sorted(set(names) ^ set(spec.fields))}")
            self.reader.compile_check(f"{spec.select.project(spec.fields)}\n AND 1 = 0")
        return f"{len(self.registry.indices)} document queries compile, in full and as field projections"

    def lookup_queries(self) -> str:
        n = 0
        for rules in self.registry.rules_by_table.values():
            for rule in rules:
                if rule.sql:
                    self.reader.compile_check(f"SELECT * FROM ({rule.sql}) WHERE 1 = 0", dict.fromkeys(rule.params))
                    n += 1
        return f"{n} lookup queries compile"

    def supplemental_logging(self) -> str:
        have = self.reader.log_group_columns(self.settings.oracle.schema)
        gaps = []
        for table, cols in self.registry.required_columns().items():
            miss = cols - have.get(table, set())
            if miss:
                gaps.append(f"{table}: {sorted(miss)}")
        if gaps:
            raise RuntimeError("columns not in an ALWAYS supplemental log group: " + "; ".join(gaps))
        return "every mapped event column is in a supplemental log group"

    def mapping_coverage(self) -> str:
        problems = unmapped_tables(self.registry)
        if problems:
            raise RuntimeError("; ".join(problems))
        return "every table used by a document query is mapped or explicitly excluded"

    def field_map(self) -> str:
        tables = {t for name in self.registry.indices for t in self.registry.tables_in_sql(name)}
        columns = self.reader.table_columns(self.settings.oracle.schema, sorted(tables))
        problems = audit_field_map(self.registry, columns)
        if problems:
            raise RuntimeError(f"{len(problems)} problems: " + " | ".join(problems[:10]))
        return "every column the queries read is mapped to the fields it feeds"
