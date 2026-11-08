"""Turns a batch of change events into Elasticsearch writes.

Idea: an event only tells us WHICH documents, and which fields of them, are affected. The new values are read
from Oracle with the same SQL the batch load uses (restricted to those fields), never taken from the event.
So the result is always the current state: re-processing an event changes nothing, and event order does not
matter. A key or join column change rebuilds the whole document; a key that Oracle no longer returns is deleted.
"""
from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from functools import partial
from typing import Any
from zoneinfo import ZoneInfo

from ..connectors.dlq import DeadLetterQueue
from ..connectors.elastic import BulkOp, ElasticsearchWriter
from ..connectors.oracle import OracleReader, is_transient
from ..core.events import Change
from ..core.keys import normalize_key
from ..core.model import ChangePlan, IndexSpec, Key, SourceRule
from ..core.registry import Registry
from ..core.transform import build_document, build_partial, key_doc_id, now_iso
from ..errors import DocumentRejectedError, MissingKeyError, ShutdownRequested
from ..lifecycle import StopSignal, backoff_delay
from ..observability.metrics import Metrics
from ..settings import SyncSettings

log = logging.getLogger(__name__)
_SEEN_CAP = 500_000      # bound on the per-batch "already rebuilt" memory

# Per batch: index -> keys rebuilt whole, and (index, fields) -> keys whose fields were refreshed.
Seen = dict[Any, set[Key]]


class SyncEngine:
    def __init__(self, registry: Registry, reader: OracleReader, writer: ElasticsearchWriter, dlq: DeadLetterQueue,
                 *, sync: SyncSettings, timezone: str, stop: StopSignal, metrics: Metrics):
        self.registry = registry
        self.reader = reader
        self.writer = writer
        self.dlq = dlq
        self.chunk = sync.chunk_size
        self.max_fanout = sync.max_fanout
        self.throttle = sync.throttle_ms / 1000
        self.workers = sync.workers
        self._pool: ThreadPoolExecutor | None = None
        self._seen_lock = threading.Lock()      # guards the per-batch `seen` sets, which the workers share
        self.max_backoff = sync.retry_max_backoff_seconds
        self.offset = sync.nested_date_offset
        # Which change types are applied: c insert, u update, d delete, r snapshot read. Others are read and skipped.
        self.operations = sync.operations
        self.tz = ZoneInfo(timezone)
        self.stop = stop
        self.metrics = metrics

    # -- public -------------------------------------------------------------------------------------------------
    def process(self, changes: list[Change]) -> None:
        """Applies a batch. Retries lost connections forever (until stopped); isolates a poison event."""
        attempt = 0
        while True:
            try:
                self._process_once(changes)
                return
            except ShutdownRequested:
                raise
            except Exception as exc:
                if is_transient(exc):
                    self.metrics.errors.labels("oracle").inc()
                    delay = backoff_delay(attempt, self.max_backoff)
                    log.warning("transient failure, retrying batch in %.1fs: %s", delay, exc)
                    attempt += 1
                    self.metrics.beat()
                    if self.stop.wait(delay):
                        raise ShutdownRequested() from exc
                    continue
                self.metrics.errors.labels("processing").inc()
                if len(changes) > 1:
                    log.error("batch failed (%s); processing its %d events one by one", exc, len(changes))
                    for change in changes:
                        self.process([change])
                    return
                self._dead_letter("processing", f"{type(exc).__name__}: {exc}", changes[0])
                return

    def resync(self, index_name: str) -> int:
        """Rebuilds every document of one index from Oracle (closes gaps, e.g. before CDC was running)."""
        spec = self.registry.indices[index_name]
        loaded_at, total = now_iso(), 0
        for rows in self.reader.stream_all(spec, self.chunk):
            ops = []
            for row in rows:
                try:
                    doc_id, doc = build_document(spec, row, self.tz, self.offset, loaded_at)
                except DocumentRejectedError as exc:
                    self.dlq.send("transform", str(exc), index=index_name, row_key=spec.key_from_row(row))
                    continue
                ops.append(BulkOp("index", spec.es_index, doc_id, doc))
            self._write(spec, ops)
            total += len(ops)
            self.metrics.beat()
            if self.stop.is_set():
                raise ShutdownRequested()
        return total

    # -- one batch ----------------------------------------------------------------------------------------------
    def _process_once(self, changes: list[Change]) -> None:
        plan = self._plan(changes)
        seen: Seen = {}
        # Each step finishes before the next starts (a whole rebuild, then partial updates, then lookups), so two
        # steps never write the same document at once; inside a step the chunks run in parallel.
        for index_name, keys in plan.rebuild.items():
            self._run(partial(self._refresh, index_name, chunk, seen) for chunk in self._chunks(sorted(keys, key=repr)))
        for index_name, by_key in plan.partial.items():
            # one projected query per distinct field set, not per document
            groups: dict[frozenset[str], list[Key]] = {}
            for key, fields in by_key.items():
                groups.setdefault(frozenset(fields), []).append(key)
            for field_set, group in groups.items():
                self._run(partial(self._refresh_fields, index_name, chunk, field_set, seen)
                          for chunk in self._chunks(sorted(group, key=repr)))
        for (_, params), pending in plan.lookups.items():
            self._expand(pending.rule, dict(params), pending.fields, seen)

    def _chunks(self, keys: list[Key]) -> Iterator[list[Key]]:
        for i in range(0, len(keys), self.chunk):
            yield keys[i:i + self.chunk]

    def _run(self, tasks: Iterable[Callable[[], Any]]) -> None:
        """Runs the tasks on the worker pool and returns when all are done. Tasks are taken lazily and at most
        2 x workers are in flight, so a lookup streaming 100,000 keys never holds more than a few chunks in memory.
        The first failure cancels what has not started, waits for what has, and is raised: the caller retries the
        batch, which is safe because every write is idempotent."""
        if self.workers <= 1:
            for task in tasks:
                task()
            return
        if self._pool is None:
            self._pool = ThreadPoolExecutor(max_workers=self.workers, thread_name_prefix="cdc-worker")
        in_flight: set[Future[Any]] = set()
        try:
            for task in tasks:
                in_flight.add(self._pool.submit(task))
                while len(in_flight) >= 2 * self.workers:
                    done, in_flight = wait(in_flight, return_when=FIRST_COMPLETED)
                    self._collect(done)
            self._collect(wait(in_flight).done)
            in_flight.clear()
        finally:
            for future in in_flight:
                future.cancel()
            wait(in_flight)

    def _collect(self, done: Iterable[Future[Any]]) -> None:
        for future in done:
            future.result()
        self.metrics.beat()

    def _plan(self, changes: list[Change]) -> ChangePlan:
        """Routes every event; duplicates collapse, so ten updates of one row cause one refresh."""
        plan = ChangePlan()
        for change in changes:
            self.metrics.events.labels(change.table, change.op).inc()
            if change.op not in self.operations:
                self.metrics.skipped.labels("operation_not_enabled").inc()
                continue
            try:
                if not self.registry.route(change, plan):
                    self.metrics.skipped.labels("unmapped_table").inc()
            except MissingKeyError as exc:
                self.metrics.skipped.labels("missing_key").inc()
                self._dead_letter("missing_key", str(exc), change)
        if plan.unchanged:
            self.metrics.skipped.labels("no_relevant_column_changed").inc(plan.unchanged)
        return plan

    def _expand(self, rule: SourceRule, params: dict[str, Any], fields: set[str] | None, seen: Seen) -> None:
        """Runs one lookup query and refreshes the documents it returns, chunk by chunk."""
        spec = self.registry.indices[rule.index]
        refresh = (partial(self._refresh, rule.index) if fields is None
                   else partial(self._refresh_fields, rule.index, fields=fields))

        def tasks() -> Iterator[Callable[[], Any]]:
            # Runs in this thread: it reads the lookup cursor and hands one chunk of keys to a worker at a time.
            total, warned = 0, False
            for raw in self.reader.lookup_chunks(rule.sql or "", params, self.chunk):
                keys = []
                for values in raw:
                    key = tuple(normalize_key(v, t) for v, t in zip(values, spec.key_types))
                    if all(v is not None for v in key):
                        keys.append(key)
                total += len(keys)
                self.metrics.fanout.labels(rule.index).inc(len(keys))
                if total > self.max_fanout:
                    if not warned:
                        log.warning("large fan-out: %s for %s rebuilds more than %d documents (%d workers)",
                                    rule.id, params, self.max_fanout, self.workers)
                        warned = True
                    if self.throttle and self.stop.wait(self.throttle):
                        raise ShutdownRequested()
                if self.stop.is_set():
                    raise ShutdownRequested()
                yield partial(refresh, keys=keys, seen=seen)

        self._run(tasks())

    def _refresh_fields(self, index_name: str, keys: list[Key], fields: Iterable[str], seen: Seen) -> None:
        """Updates only `fields` of the documents for these keys, with the current values from Oracle."""
        spec = self.registry.indices[index_name]
        wanted = sorted(fields)
        with self._seen_lock:       # claim the keys first, so a parallel worker never repeats them
            rebuilt = seen.setdefault(index_name, set())
            done = seen.setdefault((index_name, tuple(wanted)), set())
            todo = [k for k in keys if k not in rebuilt and k not in done]
            if len(done) < _SEEN_CAP:
                done.update(todo)
        for i in range(0, len(todo), self.chunk):
            chunk = todo[i:i + self.chunk]
            present: set[Key] = set()
            ops: list[BulkOp] = []
            for row in self.reader.fetch_fields(spec, chunk, wanted):
                key = spec.key_from_row(row)
                if key is None:
                    continue
                present.add(key)
                try:
                    doc_id, part = build_partial(spec, row, wanted, self.tz, self.offset)
                except DocumentRejectedError as exc:
                    self.dlq.send("transform", str(exc), index=index_name, key=repr(key))
                    continue
                ops.append(BulkOp("update", spec.es_index, doc_id, part, key))
            ops.extend(self._deletes(spec, chunk, present))
            missing = self._write(spec, ops)
            if missing:
                log.info("%d %s documents were not in Elasticsearch; rebuilding them whole", len(missing), index_name)
                self._refresh(index_name, missing, seen)

    def _refresh(self, index_name: str, keys: list[Key], seen: Seen) -> None:
        """Rebuilds the whole documents for these keys from the full document query."""
        spec = self.registry.indices[index_name]
        with self._seen_lock:       # claim the keys first, so a parallel worker never repeats them
            done = seen.setdefault(index_name, set())
            todo = [k for k in keys if k not in done]
            if len(done) < _SEEN_CAP:
                done.update(todo)
        for i in range(0, len(todo), self.chunk):
            chunk = todo[i:i + self.chunk]
            present: set[Key] = set()
            ops: list[BulkOp] = []
            for row in self.reader.fetch_rows(spec, chunk):
                key = spec.key_from_row(row)
                if key is not None:
                    present.add(key)
                try:
                    doc_id, doc = build_document(spec, row, self.tz, self.offset)
                except DocumentRejectedError as exc:
                    self.dlq.send("transform", str(exc), index=index_name, key=repr(key))
                    continue
                ops.append(BulkOp("index", spec.es_index, doc_id, doc))
            ops.extend(self._deletes(spec, chunk, present))
            self._write(spec, ops)

    def _deletes(self, spec: IndexSpec, chunk: list[Key], present: set[Key]) -> list[BulkOp]:
        """A key Oracle no longer returns: its document is deleted."""
        return [BulkOp("delete", spec.es_index, key_doc_id(spec, key, self.tz)) for key in chunk if key not in present]

    def _write(self, spec: IndexSpec, ops: list[BulkOp]) -> list[Key]:
        """Applies the ops. Returns the keys of partial updates whose document does not exist (rebuild those)."""
        if not ops:
            return []
        missing = []
        for op, reason in self.writer.apply(spec.target, ops):
            if op.action == "update" and "document_missing" in reason and op.key is not None:
                missing.append(op.key)
                continue
            self.dlq.send("elasticsearch", reason, index=op.index, doc_id=op.doc_id, action=op.action)
        return missing

    def _dead_letter(self, stage: str, reason: str, change: Change) -> None:
        self.dlq.send(stage, reason, table=change.table, op=change.op, topic=change.topic,
                      partition=change.partition, offset=change.offset, before=change.before, after=change.after)
