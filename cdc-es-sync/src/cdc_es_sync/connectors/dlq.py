"""Dead letter output: a Kafka topic, with a local file when the topic cannot be written."""
from __future__ import annotations

import json
import logging
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from confluent_kafka import Producer

from ..observability.metrics import Metrics

log = logging.getLogger(__name__)


class DeadLetterQueue:
    def __init__(self, producer: Producer | None, topic: str, fallback_dir: Path, metrics: Metrics):
        self.producer = producer
        self.topic = topic
        self.dir = Path(fallback_dir)
        self.metrics = metrics
        self.count = 0
        self._lock = threading.Lock()       # the engine's workers may dead-letter at the same time

    def send(self, stage: str, reason: str, **detail: Any) -> None:
        """Records one item that could not be applied: to the topic, or to the fallback file if that fails."""
        with self._lock:
            self._send(stage, reason, detail)

    def _send(self, stage: str, reason: str, detail: dict[str, Any]) -> None:
        record = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "stage": stage,
            "reason": reason,
            **detail,
        }
        self.count += 1
        self.metrics.dlq.labels(stage).inc()
        log.error("dead letter: %s: %s", stage, reason, extra={"ctx": {"detail": detail}})
        payload = json.dumps(record, default=str)
        try:
            if self.producer is None:
                raise RuntimeError("no producer")
            self.producer.produce(self.topic, payload.encode("utf-8"))
            if self.producer.flush(10) > 0:
                raise RuntimeError("DLQ topic write timed out")
        except Exception as exc:
            log.warning("DLQ topic unavailable (%s); writing to file", exc)
            self._write_file(payload)

    def _write_file(self, payload: str) -> None:
        self.dir.mkdir(parents=True, exist_ok=True)
        with open(self.dir / f"dlq-{time.strftime('%Y%m%d')}.ndjson", "a", encoding="utf-8") as fh:
            fh.write(payload + "\n")
