"""Bulk writes to Elasticsearch, retried until they succeed, with per-document failures reported."""
from __future__ import annotations

import logging
from dataclasses import dataclass

import elasticsearch as es

from .util import ShutdownRequested, backoff_delay

log = logging.getLogger("es")
_RETRY_STATUS = {429, 502, 503, 504}


@dataclass
class Op:
    action: str            # "index" (whole document), "update" (only the given fields) or "delete"
    index: str
    doc_id: str
    doc: dict | None = None
    key: tuple | None = None        # the document key, so a failed partial update can be rebuilt


def make_client(target: dict, **kwargs) -> es.Elasticsearch:
    """Client for one `elasticsearch.targets` entry: login (`username`/`password`) and the CA file (`ca_certs`) are optional."""
    opts = dict(kwargs)
    opts.setdefault("request_timeout", target.get("request_timeout", 120))
    if target.get("username"):
        opts["basic_auth"] = (target["username"], target.get("password", ""))
    if target.get("ca_certs"):
        opts["ca_certs"] = target["ca_certs"]
    return es.Elasticsearch(target["hosts"], **opts)


class EsWriter:
    def __init__(self, targets: dict, stop, metrics, max_backoff: float = 30.0, max_actions: int = 1000):
        self.clients = {name: make_client(t, max_retries=0) for name, t in targets.items()}
        self.stop = stop
        self.metrics = metrics
        self.max_backoff = max_backoff
        self.max_actions = max_actions

    def client(self, target: str):
        return self.clients[target]

    def apply(self, target: str, ops: list[Op]) -> list[tuple[Op, str]]:
        """Applies all ops. Returns (op, reason) for documents Elasticsearch refused for good."""
        failed = []
        for i in range(0, len(ops), self.max_actions):
            failed.extend(self._bulk(target, ops[i:i + self.max_actions]))
        return failed

    def _bulk(self, target: str, ops: list[Op]) -> list[tuple[Op, str]]:
        client = self.clients[target]
        pending = list(ops)
        failed: list[tuple[Op, str]] = []
        attempt = 0
        while pending:
            body = []
            for op in pending:
                meta = {"_index": op.index, "_id": op.doc_id}
                if op.action == "update":
                    meta["retry_on_conflict"] = 3
                body.append({op.action: meta})
                if op.action == "index":
                    body.append(op.doc)
                elif op.action == "update":
                    body.append({"doc": op.doc})
            try:
                result = client.bulk(operations=body, refresh=False)
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
                    counter = {"index": self.metrics.indexed, "update": self.metrics.updated}.get(op.action, self.metrics.deleted)
                    counter.labels(op.index).inc()
                elif status in _RETRY_STATUS:
                    retry.append(op)
                else:
                    failed.append((op, str(status_item.get("error"))[:500]))
            if retry:
                self._wait(attempt, f"{len(retry)} bulk items rejected with a retryable status")
                attempt += 1
            pending = retry
        return failed

    def _wait(self, attempt: int, why: str):
        self.metrics.errors.labels("elasticsearch").inc()
        delay = backoff_delay(attempt, self.max_backoff)
        log.warning("%s; retrying in %.1fs (attempt %d)", why, delay, attempt + 1)
        if self.stop.wait(delay):
            raise ShutdownRequested()

    def ping_all(self):
        for name, client in self.clients.items():
            if not client.ping():
                raise ConnectionError(f"Elasticsearch target {name} did not answer")

    def index_exists(self, target: str, index: str) -> bool:
        return bool(self.clients[target].indices.exists(index=index))
