"""Composition root: builds every component from the settings and owns their lifecycle.

    Kafka (change events) --> ChangeConsumer --> SyncEngine --> ElasticsearchWriter --> Elasticsearch
                                                     |
                                                     +--> OracleReader (current values)    DeadLetterQueue (failures)
"""
from __future__ import annotations

import logging

from .connectors.dlq import DeadLetterQueue
from .connectors.elastic import ElasticsearchWriter
from .connectors.kafka import create_consumer, create_producer, ensure_topic
from .connectors.oracle import OracleReader
from .core.registry import Registry
from .lifecycle import StopSignal
from .observability.metrics import Metrics, serve
from .pipeline.consumer import ChangeConsumer
from .pipeline.engine import SyncEngine
from .pipeline.preflight import CheckResult, Preflight
from .pipeline.reconcile import reconcile
from .settings import Settings

log = logging.getLogger(__name__)

EXIT_OK, EXIT_FAILED, EXIT_PRECONDITION = 0, 1, 2


class Application:
    def __init__(self, settings: Settings, stop: StopSignal | None = None):
        self.settings = settings
        self.stop = stop or StopSignal()
        self.metrics = Metrics()
        self.registry = Registry.from_settings(settings)
        self.producer = create_producer(settings.kafka)
        self.dlq = DeadLetterQueue(self.producer, settings.kafka.dlq_topic,
                                   settings.observability.dlq_file_dir, self.metrics)
        self.reader = OracleReader(settings.oracle)
        self.writer = ElasticsearchWriter(settings.elasticsearch.targets, self.stop, self.metrics,
                                          max_backoff=settings.sync.retry_max_backoff_seconds,
                                          max_actions=settings.sync.bulk_max_actions)
        self.engine = SyncEngine(self.registry, self.reader, self.writer, self.dlq,
                                 sync=settings.sync, timezone=settings.oracle.timezone,
                                 stop=self.stop, metrics=self.metrics)
        self.preflight = Preflight(settings, self.registry, self.reader, self.writer)

    # -- commands -----------------------------------------------------------------------------------------------
    def check(self) -> list[CheckResult]:
        return self.preflight.run()

    def reconcile(self) -> tuple[list[dict], bool]:
        return reconcile(self.registry, self.reader, self.writer)

    def resync(self, index: str) -> int:
        return self.engine.resync(index)

    def run(self) -> int:
        """Serves /health and /metrics, runs the start-up checks, then consumes until stopped. Returns an exit code."""
        obs = self.settings.observability
        serve(self.metrics, obs.http_port, obs.health_stale_seconds)
        if not self._startup_checks_pass():
            self.metrics.beat("failed")
            return EXIT_PRECONDITION
        ensure_topic(self.settings.kafka, self.settings.kafka.dlq_topic)
        consumer = create_consumer(self.settings.kafka)
        consumer.subscribe(
            [self.settings.kafka.topic_pattern],
            on_assign=lambda c, p: log.info("assigned %s", [f"{x.topic}[{x.partition}]" for x in p]),
            on_revoke=lambda c, p: log.info("revoked %d partitions", len(p)),
        )
        self.metrics.beat("running")
        log.info("cdc-es-sync running")
        try:
            ChangeConsumer(consumer, self.engine, self.dlq, self.metrics, self.stop, self.settings.batch).run()
        finally:
            consumer.close()
            self.close()
            self.metrics.beat("stopped")
            log.info("stopped")
        return EXIT_OK

    def close(self) -> None:
        self.producer.flush(5)
        self.reader.close()

    def _startup_checks_pass(self) -> bool:
        results = self.check()
        for result in results:
            if result.ok:
                log.info("start-up check ok: %s: %s", result.name, result.detail)
            else:
                log.error("start-up check failed: %s: %s", result.name, result.detail)
        return not any(self.preflight.is_fatal(r) for r in results)
