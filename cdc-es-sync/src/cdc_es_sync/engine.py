"""Turns a batch of change events into Elasticsearch writes.

Idea: an event only tells us WHICH documents, and which fields of them, are affected. The new values are read
from Oracle with the same SQL the batch load uses (restricted to those fields), never taken from the event.
So the result is always the current state: re-processing an event changes nothing, and event order does not
matter. A key or join column change rebuilds the whole document; a key that Oracle no longer returns is deleted.
"""
from __future__ import annotations

import logging
from zoneinfo import ZoneInfo

from .es_writer import Op
from .mapping import MissingKeyError, Plan, norm
from .oracle_reader import is_transient
from .transform import RejectError, build_document, build_partial, key_doc_id, now_iso
from .util import ShutdownRequested, backoff_delay

log = logging.getLogger("engine")
_SEEN_CAP = 500_000      # bound on the per-batch "already rebuilt" memory


class Engine:
    def __init__(self, registry, reader, writer, dlq, sync_cfg: dict, tz_name: str, stop, metrics):
        self.registry = registry
        self.reader = reader
        self.writer = writer
        self.dlq = dlq
        self.chunk = int(sync_cfg.get("chunk_size", 500))
        self.max_fanout = int(sync_cfg.get("max_fanout", 50000))
        self.throttle = float(sync_cfg.get("throttle_ms", 200)) / 1000
        self.max_backoff = float(sync_cfg.get("retry_max_backoff_seconds", 30))
        self.offset = sync_cfg.get("nested_date_offset", "+06:00")
        self.tz = ZoneInfo(tz_name)
        self.stop = stop
        self.metrics = metrics

    # -- public ---------------------------------------------------------------------------------------
    def process(self, changes: list):
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
                        raise ShutdownRequested()
                    continue
                self.metrics.errors.labels("processing").inc()
                if len(changes) > 1:
                    log.error("batch failed (%s); processing its %d events one by one", exc, len(changes))
                    for change in changes:
                        self.process([change])
                    return
                change = changes[0]
                self.dlq.send("processing", f"{type(exc).__name__}: {exc}", table=change.table, op=change.op,
                              topic=change.topic, partition=change.partition, offset=change.offset,
                              before=change.before, after=change.after)
                return

    def resync(self, index_name: str) -> int:
        """Rebuilds every document of one index from Oracle (closes gaps, e.g. before CDC was running)."""
        ix = self.registry.indices[index_name]
        loaded_at, total = now_iso(), 0
        for rows in self.reader.stream_all(ix, self.chunk):
            ops = []
            for row in rows:
                try:
                    doc_id, doc = build_document(ix, row, self.tz, self.offset, loaded_at)
                except RejectError as exc:
                    self.dlq.send("transform", str(exc), index=index_name, row_key=ix.key_from_row(row))
                    continue
                ops.append(Op("index", ix.es_index, doc_id, doc))
            self._write(ix, ops)
            total += len(ops)
            self.metrics.beat()
            if self.stop.is_set():
                raise ShutdownRequested()
        return total

    # -- internals ------------------------------------------------------------------------------------
    def _process_once(self, changes: list):
        plan = Plan()
        for change in changes:
            self.metrics.events.labels(change.table, change.op).inc()
            try:
                if not self.registry.route(change, plan):
                    self.metrics.skipped.labels("unmapped_table").inc()
            except MissingKeyError as exc:
                self.metrics.skipped.labels("missing_key").inc()
                self.dlq.send("missing_key", str(exc), table=change.table, op=change.op, topic=change.topic,
                              partition=change.partition, offset=change.offset,
                              before=change.before, after=change.after)
        if plan.unchanged:
            self.metrics.skipped.labels("no_relevant_column_changed").inc(plan.unchanged)
        seen: dict = {}
        for index_name, keys in plan.full.items():
            self._refresh(index_name, sorted(keys, key=repr), seen)
        for index_name, by_key in plan.part.items():
            groups: dict = {}
            for key, fields in by_key.items():
                groups.setdefault(frozenset(fields), []).append(key)
            for fields, keys in groups.items():
                self._refresh_fields(index_name, sorted(keys, key=repr), fields, seen)
        for (_, params), (impact, fields) in plan.lookups.items():
            self._expand(impact, dict(params), fields, seen)

    def _expand(self, impact, params: dict, fields, seen: dict):
        ix = self.registry.indices[impact.index]
        total, warned = 0, False
        for raw in self.reader.lookup_chunks(impact.sql, params, self.chunk):
            keys = []
            for values in raw:
                key = tuple(norm(v, t) for v, t in zip(values, ix.key_types))
                if all(v is not None for v in key):
                    keys.append(key)
            total += len(keys)
            self.metrics.fanout.labels(impact.index).inc(len(keys))
            if total > self.max_fanout:
                if not warned:
                    log.warning("large fan-out: %s for %s rebuilds more than %d documents; throttling",
                                impact.id, params, self.max_fanout)
                    warned = True
                self.metrics.beat()
                if self.stop.wait(self.throttle):
                    raise ShutdownRequested()
            if fields is None:
                self._refresh(impact.index, keys, seen)
            else:
                self._refresh_fields(impact.index, keys, fields, seen)

    def _refresh_fields(self, index_name: str, keys: list, fields, seen: dict):
        """Updates only `fields` of the documents for these keys, with the current values from Oracle."""
        ix = self.registry.indices[index_name]
        wanted = sorted(fields)
        rebuilt = seen.setdefault(index_name, set())
        done = seen.setdefault((index_name, tuple(wanted)), set())
        todo = [k for k in keys if k not in rebuilt and k not in done]
        for i in range(0, len(todo), self.chunk):
            chunk = todo[i:i + self.chunk]
            present, ops = set(), []
            for row in self.reader.fetch_fields(ix, chunk, wanted):
                key = ix.key_from_row(row)
                if key is None:
                    continue
                present.add(key)
                try:
                    doc_id, part = build_partial(ix, row, wanted, self.tz, self.offset)
                except RejectError as exc:
                    self.dlq.send("transform", str(exc), index=index_name, key=repr(key))
                    continue
                ops.append(Op("update", ix.es_index, doc_id, part, key))
            for key in chunk:
                if key not in present:
                    ops.append(Op("delete", ix.es_index, key_doc_id(ix, key, self.tz)))
            missing = self._write(ix, ops)
            if missing:
                log.info("%d %s documents were not in Elasticsearch; rebuilding them whole", len(missing), index_name)
                self._refresh(index_name, missing, seen)
            if len(done) < _SEEN_CAP:
                done.update(chunk)

    def _refresh(self, index_name: str, keys: list, seen: dict):
        ix = self.registry.indices[index_name]
        done = seen.setdefault(index_name, set())
        todo = [k for k in keys if k not in done]
        for i in range(0, len(todo), self.chunk):
            chunk = todo[i:i + self.chunk]
            rows = self.reader.fetch_rows(ix, chunk)
            present, ops = set(), []
            for row in rows:
                key = ix.key_from_row(row)
                if key is not None:
                    present.add(key)
                try:
                    doc_id, doc = build_document(ix, row, self.tz, self.offset)
                except RejectError as exc:
                    self.dlq.send("transform", str(exc), index=index_name, key=repr(key))
                    continue
                ops.append(Op("index", ix.es_index, doc_id, doc))
            for key in chunk:
                if key not in present:
                    ops.append(Op("delete", ix.es_index, key_doc_id(ix, key, self.tz)))
            self._write(ix, ops)
            if len(done) < _SEEN_CAP:
                done.update(chunk)

    def _write(self, ix, ops: list) -> list:
        """Applies the ops. Returns the keys of partial updates whose document does not exist (rebuild those)."""
        if not ops:
            return []
        missing = []
        for op, reason in self.writer.apply(ix.target, ops):
            if op.action == "update" and "document_missing" in reason and op.key is not None:
                missing.append(op.key)
                continue
            self.dlq.send("elasticsearch", reason, index=op.index, doc_id=op.doc_id, action=op.action)
        return missing
