"""The Kafka consume loop: collect a batch, apply it, then commit its offsets (at-least-once)."""
from __future__ import annotations

import logging
import time
from typing import cast

from confluent_kafka import Consumer, KafkaError, KafkaException, Message, TopicPartition

from ..connectors.dlq import DeadLetterQueue
from ..connectors.kafka import partition_lag
from ..core.events import Change, parse_event
from ..errors import InvalidEventError, ShutdownRequested
from ..lifecycle import StopSignal
from ..observability.metrics import Metrics
from ..settings import BatchSettings
from .engine import SyncEngine

log = logging.getLogger(__name__)

LAG_INTERVAL_SECONDS = 15
UNASSIGNED_TIMEOUT_SECONDS = 120


class ChangeConsumer:
    def __init__(self, consumer: Consumer, engine: SyncEngine, dlq: DeadLetterQueue, metrics: Metrics,
                 stop: StopSignal, batch: BatchSettings):
        self.consumer = consumer
        self.engine = engine
        self.dlq = dlq
        self.metrics = metrics
        self.stop = stop
        self.batch = batch

    def run(self) -> None:
        """Consumes until stopped. Raises KafkaException on a fatal consumer error."""
        last_lag = 0.0
        unassigned_since: float | None = time.monotonic()
        try:
            while not self.stop.is_set():
                messages = self.collect()
                unassigned_since = self._check_assignment(unassigned_since)
                if messages:
                    self.handle(messages)
                if time.time() - last_lag > LAG_INTERVAL_SECONDS:
                    self.report_lag()
                    last_lag = time.time()
        except ShutdownRequested:
            log.info("shutdown requested; uncommitted events will be re-read on the next start")

    def _check_assignment(self, unassigned_since: float | None) -> float | None:
        """Fail a process that polls but never belongs to the group, so Docker can restart it.

        librdkafka normally rejoins after a broker restart. If it remains detached, consume() can keep returning an
        empty list forever; refreshing the ordinary heartbeat in that state made /health report a false positive.
        """
        now = time.monotonic()
        if self.consumer.assignment():
            self.metrics.beat()
            return None
        if unassigned_since is None:
            unassigned_since = now
        if now - unassigned_since >= UNASSIGNED_TIMEOUT_SECONDS:
            self.metrics.beat("failed")
            raise RuntimeError(
                f"Kafka consumer has had no partition assignment for {UNASSIGNED_TIMEOUT_SECONDS}s; restarting"
            )
        return unassigned_since

    def collect(self) -> list[Message]:
        """Up to `batch.max_events` messages, or whatever arrived within `batch.max_wait_seconds`."""
        max_events, max_wait = self.batch.max_events, self.batch.max_wait_seconds
        messages: list[Message] = []
        deadline = time.time() + max_wait
        while len(messages) < max_events and time.time() < deadline and not self.stop.is_set():
            for msg in self.consumer.consume(num_messages=max_events - len(messages), timeout=min(0.25, max_wait)):
                error = msg.error()
                if error:
                    if error.code() in (KafkaError._PARTITION_EOF, KafkaError.UNKNOWN_TOPIC_OR_PART):
                        continue
                    raise KafkaException(error)
                messages.append(msg)
        return messages

    def handle(self, messages: list[Message]) -> None:
        """Parses, applies and commits one batch. Nothing is committed if applying it raised."""
        started = time.time()
        changes: list[Change] = []
        high: dict[tuple[str, int], int] = {}
        for msg in messages:
            # set on every consumed message; the client types them as optional
            topic, partition, offset = cast(str, msg.topic()), cast(int, msg.partition()), cast(int, msg.offset())
            high[(topic, partition)] = max(high.get((topic, partition), -1), offset)
            try:
                change = parse_event(msg.value(), topic, partition, offset)
            except InvalidEventError as exc:
                self.metrics.skipped.labels("bad_event").inc()
                self.dlq.send("parse", str(exc), topic=topic, partition=partition, offset=offset,
                              value=(msg.value() or b"")[:2000].decode("utf-8", "replace"))
                continue
            if change is None:
                self.metrics.skipped.labels("no_row_change").inc()
            else:
                changes.append(change)
        if changes:
            self.engine.process(changes)
        self.consumer.commit(offsets=[TopicPartition(t, p, o + 1) for (t, p), o in high.items()], asynchronous=False)
        elapsed = time.time() - started
        self.metrics.batches.inc()
        self.metrics.batch_seconds.observe(elapsed)
        self.metrics.last_batch.set(time.time())
        log.info("batch committed: %d messages, %d row changes, %.2fs", len(messages), len(changes), elapsed)

    def report_lag(self) -> None:
        try:
            for tp in self.consumer.assignment():
                low, high = self.consumer.get_watermark_offsets(tp, timeout=5, cached=False)
                pos = self.consumer.position([tp])[0].offset
                committed = self.consumer.committed([tp], timeout=5)[0].offset if pos < 0 else -1
                self.metrics.lag.labels(tp.topic, str(tp.partition)).set(partition_lag(low, high, pos, committed))
        except Exception as exc:
            log.debug("lag check failed: %s", exc)
