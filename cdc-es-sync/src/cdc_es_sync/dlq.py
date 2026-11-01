"""Dead letter output: a Kafka topic, with a local file when the topic cannot be written."""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger("dlq")


class Dlq:
    def __init__(self, producer, topic: str, fallback_dir: Path, metrics):
        self.producer = producer
        self.topic = topic
        self.dir = Path(fallback_dir)
        self.metrics = metrics
        self.count = 0

    def send(self, stage: str, reason: str, **detail):
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
            self.dir.mkdir(parents=True, exist_ok=True)
            with open(self.dir / f"dlq-{time.strftime('%Y%m%d')}.ndjson", "a", encoding="utf-8") as fh:
                fh.write(payload + "\n")
