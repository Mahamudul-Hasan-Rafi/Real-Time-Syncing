"""Prometheus metrics plus a /health endpoint, served from one small HTTP thread."""
from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST


class Metrics:
    def __init__(self):
        self.registry = CollectorRegistry()
        r = self.registry
        self.events = Counter("cdc_events_total", "Change events read", ["table", "op"], registry=r)
        self.skipped = Counter("cdc_events_skipped_total", "Events not applied", ["reason"], registry=r)
        self.indexed = Counter("cdc_docs_indexed_total", "Documents written", ["index"], registry=r)
        self.updated = Counter("cdc_docs_updated_total", "Documents with some fields updated", ["index"], registry=r)
        self.deleted = Counter("cdc_docs_deleted_total", "Documents deleted", ["index"], registry=r)
        self.fanout = Counter("cdc_fanout_keys_total", "Keys resolved through lookup queries", ["index"], registry=r)
        self.errors = Counter("cdc_errors_total", "Errors by stage", ["stage"], registry=r)
        self.dlq = Counter("cdc_dlq_total", "Items sent to the dead letter topic", ["stage"], registry=r)
        self.batches = Counter("cdc_batches_total", "Batches committed", registry=r)
        self.batch_seconds = Histogram("cdc_batch_seconds", "Batch processing time", registry=r)
        self.lag = Gauge("cdc_consumer_lag", "Messages behind the log end", ["topic", "partition"], registry=r)
        self.last_batch = Gauge("cdc_last_batch_timestamp", "Unix time of the last committed batch", registry=r)
        self._lock = threading.Lock()
        self._beat = time.time()
        self._state = "starting"

    def beat(self, state: str | None = None):
        with self._lock:
            self._beat = time.time()
            if state:
                self._state = state

    def health(self, stale_seconds: float) -> tuple[bool, dict]:
        with self._lock:
            age = time.time() - self._beat
            ok = age < stale_seconds and self._state != "failed"
            return ok, {"state": self._state, "seconds_since_heartbeat": round(age, 1)}


def serve(metrics: Metrics, port: int, stale_seconds: float):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path.startswith("/metrics"):
                body, ctype, code = generate_latest(metrics.registry), CONTENT_TYPE_LATEST, 200
            elif self.path.startswith("/health"):
                ok, detail = metrics.health(stale_seconds)
                body, ctype, code = json.dumps(detail).encode(), "application/json", 200 if ok else 503
            else:
                body, ctype, code = b"not found", "text/plain", 404
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    threading.Thread(target=server.serve_forever, name="metrics-http", daemon=True).start()
    return server
