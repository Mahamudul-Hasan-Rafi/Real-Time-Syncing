"""Bulk writes to Elasticsearch, retried until they succeed, with per-document failures reported."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import elasticsearch as es

from ..errors import ShutdownRequested
from ..lifecycle import StopSignal, backoff_delay
from ..observability.metrics import Metrics
from ..settings import ElasticsearchTarget

log = logging.getLogger(__name__)
_RETRY_STATUS = {429, 502, 503, 504}


@dataclass
class BulkOp:
    action: str            # "index" (whole document), "update" (only the given fields) or "delete"
    index: str
    doc_id: str
    doc: dict[str, Any] | None = None
    key: tuple | None = None        # the document key, so a failed partial update can be rebuilt


def make_client(target: ElasticsearchTarget, **kwargs: Any) -> es.Elasticsearch:
    """Client for one `elasticsearch.targets` entry: login and the CA file are optional."""
    opts = dict(kwargs)
    opts.setdefault("request_timeout", target.request_timeout)
    if target.username:
        opts["basic_auth"] = (target.username, target.password)
    if not target.verify_certs:
        opts["verify_certs"] = False
        opts["ssl_show_warn"] = False
    elif target.ca_certs:
        opts["ca_certs"] = target.ca_certs
    return es.Elasticsearch(list(target.hosts), **opts)


def _bulk_body(ops: list[BulkOp]) -> list[dict[str, Any]]:
    body: list[dict[str, Any]] = []
    for op in ops:
        meta: dict[str, Any] = {"_index": op.index, "_id": op.doc_id}
        if op.action == "update":
            meta["retry_on_conflict"] = 3
        body.append({op.action: meta})
        if op.action == "index":
            body.append(op.doc or {})
        elif op.action == "update":
            body.append({"doc": op.doc or {}})
    return body


class ElasticsearchWriter:
    def __init__(self, targets: dict[str, ElasticsearchTarget], stop: StopSignal, metrics: Metrics,
                 max_backoff: float = 30.0, max_actions: int = 1000):
        self.clients = {name: make_client(t, max_retries=0) for name, t in targets.items()}
        self.stop = stop
        self.metrics = metrics
        self.max_backoff = max_backoff
        self.max_actions = max_actions

    def client(self, target: str) -> es.Elasticsearch:
        return self.clients[target]

    def apply(self, target: str, ops: list[BulkOp]) -> list[tuple[BulkOp, str]]:
        """Applies all ops. Returns (op, reason) for documents Elasticsearch refused for good."""
        failed = []
        for i in range(0, len(ops), self.max_actions):
            failed.extend(self._bulk(target, ops[i:i + self.max_actions]))
        return failed

    def _bulk(self, target: str, ops: list[BulkOp]) -> list[tuple[BulkOp, str]]:
        """One bulk request, retried (whole or per item) while Elasticsearch is unreachable or overloaded."""
        client = self.clients[target]
        pending = list(ops)
        failed: list[tuple[BulkOp, str]] = []
        attempt = 0
        while pending:
            try:
                result = client.bulk(operations=_bulk_body(pending), refresh=False)
            except (es.ConnectionError, es.ConnectionTimeout) as exc:
                self._wait(attempt, f"Elasticsearch unreachable: {exc}")
                attempt += 1
                continue
            except es.ApiError as exc:
                if exc.status_code in _RETRY_STATUS:
                    self._wait(attempt, f"Elasticsearch returned {exc.status_code}")
                    attempt += 1
                    continue
                raise
            retry = []
            for op, item in zip(pending, result["items"]):
                status_item = next(iter(item.values()))
                status = status_item.get("status", 500)
                if status < 300 or (op.action == "delete" and status == 404):
                    self._count(op)
                elif status in _RETRY_STATUS:
                    retry.append(op)
                else:
                    failed.append((op, str(status_item.get("error"))[:500]))
            if retry:
                self._wait(attempt, f"{len(retry)} bulk items rejected with a retryable status")
                attempt += 1
            pending = retry
        return failed

    def _count(self, op: BulkOp) -> None:
        counter = {"index": self.metrics.indexed, "update": self.metrics.updated}.get(op.action, self.metrics.deleted)
        counter.labels(op.index).inc()

    def _wait(self, attempt: int, why: str) -> None:
        self.metrics.errors.labels("elasticsearch").inc()
        delay = backoff_delay(attempt, self.max_backoff)
        log.warning("%s; retrying in %.1fs (attempt %d)", why, delay, attempt + 1)
        if self.stop.wait(delay):
            raise ShutdownRequested()

    # -- start-up checks ----------------------------------------------------------------------------------------
    def ping_all(self) -> None:
        for name, client in self.clients.items():
            if not client.ping():
                raise ConnectionError(f"Elasticsearch target {name} did not answer")

    def index_exists(self, target: str, index: str) -> bool:
        return bool(self.clients[target].indices.exists(index=index))
