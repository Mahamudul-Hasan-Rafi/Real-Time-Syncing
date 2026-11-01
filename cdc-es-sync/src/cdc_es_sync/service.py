"""Kafka consume loop, start-up checks and wiring."""
from __future__ import annotations

import logging
import time
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from .dlq import Dlq
from .engine import Engine
from .es_writer import EsWriter
from .events import BadEvent, parse_event
from .mapping import Registry
from .metrics import Metrics, serve
from .oracle_reader import OracleReader
from .util import ShutdownRequested, Stop

log = logging.getLogger("service")


class Service:
    def __init__(self, cfg: dict, stop: Stop | None = None):
        self.cfg = cfg
        self.stop = stop or Stop()
        self.metrics = Metrics()
        self.registry = Registry.from_config(cfg)
        k = cfg["kafka"]
        self.producer = Producer({"bootstrap.servers": k["bootstrap_servers"], "client.id": "cdc-es-sync-dlq"})
        base = Path(cfg["_base"])
        self.dlq = Dlq(self.producer, k["dlq_topic"], base / cfg["observability"].get("dlq_file_dir", "dlq"), self.metrics)
        self.reader = OracleReader(cfg["oracle"])
        sync = cfg["sync"]
        self.writer = EsWriter(cfg["elasticsearch"]["targets"], self.stop, self.metrics,
                               max_backoff=float(sync.get("retry_max_backoff_seconds", 30)),
                               max_actions=int(sync.get("bulk_max_actions", 1000)))
        self.engine = Engine(self.registry, self.reader, self.writer, self.dlq, sync,
                             cfg["oracle"].get("timezone", "UTC"), self.stop, self.metrics)

    # -- start-up checks (also `python -m cdc_es_sync check`) ----------------------------------------
    def checks(self) -> list[tuple[str, bool, str]]:
        results = []

        def run(name, fn):
            try:
                results.append((name, True, fn() or "ok"))
            except Exception as exc:
                results.append((name, False, f"{type(exc).__name__}: {exc}"))

        k = self.cfg["kafka"]

        def kafka():
            md = AdminClient({"bootstrap.servers": k["bootstrap_servers"]}).list_topics(timeout=10)
            return f"{len(md.topics)} topics"
        run("kafka reachable", kafka)

        def es_up():
            self.writer.ping_all()
            return "all targets answer"
        run("elasticsearch reachable", es_up)

        def indexes():
            missing = [ix.es_index for ix in self.registry.indices.values()
                       if not self.writer.index_exists(ix.target, ix.es_index)]
            if missing:
                raise RuntimeError(f"index missing: {missing}")
            return "all indices exist"
        run("elasticsearch indices", indexes)

        run("oracle reachable", lambda: self.reader.ping() or "ok")

        def document_sql():
            for ix in self.registry.indices.values():
                names = self.reader.output_names(f"{ix.sql}\n AND 1 = 0")
                if set(names) != set(ix.fields):
                    raise RuntimeError(f"{ix.name}: parsed output columns differ from Oracle's: "
                                       f"{sorted(set(names) ^ set(ix.fields))}")
                self.reader.compile_check(f"{ix.select.project(ix.fields)}\n AND 1 = 0")
            return f"{len(self.registry.indices)} document queries compile, in full and as field projections"
        run("document queries", document_sql)

        def lookup_sql():
            n = 0
            for impacts in self.registry.tables.values():
                for imp in impacts:
                    if imp.sql:
                        self.reader.compile_check(f"SELECT * FROM ({imp.sql}) WHERE 1 = 0", {p: None for p in imp.params})
                        n += 1
            return f"{n} lookup queries compile"
        run("lookup queries", lookup_sql)

        def logging_groups():
            have = self.reader.log_group_columns(self.cfg["oracle"].get("schema", "EMOB"))
            gaps = []
            for table, cols in self.registry.required_columns().items():
                miss = cols - have.get(table, set())
                if miss:
                    gaps.append(f"{table}: {sorted(miss)}")
            if gaps:
                raise RuntimeError("columns not in an ALWAYS supplemental log group: " + "; ".join(gaps))
            return "every mapped event column is in a supplemental log group"
        run("supplemental logging", logging_groups)

        def coverage():
            problems = []
            for name, ix in self.registry.indices.items():
                mapped = {t for t, imps in self.registry.tables.items() if any(i.index == name for i in imps)}
                referenced = self.registry.tables_in_sql(name)
                loose = referenced - mapped - set(ix.not_propagated)
                if loose:
                    problems.append(f"{name}: tables in the SQL with no mapping and no exclusion: {sorted(loose)}")
            if problems:
                raise RuntimeError("; ".join(problems))
            return "every table used by a document query is mapped or explicitly excluded"
        run("mapping coverage", coverage)

        def field_map():
            schema = self.cfg["oracle"].get("schema", "EMOB")
            tables = {t for name in self.registry.indices for t in self.registry.tables_in_sql(name)}
            problems = self.registry.audit(self.reader.table_columns(schema, sorted(tables)))
            if problems:
                raise RuntimeError(f"{len(problems)} problems: " + " | ".join(problems[:10]))
            return "every column the queries read is mapped to the fields it feeds"
        run("field map", field_map)
        return results

    # -- main loop --------------------------------------------------------------------------------
    def run(self):
        obs = self.cfg["observability"]
        serve(self.metrics, int(obs.get("http_port", 9108)), float(obs.get("health_stale_seconds", 120)))
        results = self.checks()
        for name, ok, detail in results:
            if ok:
                log.info("start-up check ok: %s: %s", name, detail)
        failed = [r for r in results if not r[1]]
        if failed:
            for name, _, detail in failed:
                log.error("start-up check failed: %s: %s", name, detail)
            fatal = [f for f in failed if f[0] != "supplemental logging"
                     or self.cfg.get("startup_checks", {}).get("fail_on_missing_logging", True)]
            if fatal:
                self.metrics.beat("failed")
                raise SystemExit(2)
        self._ensure_dlq_topic()
        k = self.cfg["kafka"]
        consumer = Consumer({
            "bootstrap.servers": k["bootstrap_servers"], "group.id": k["group_id"],
            "enable.auto.commit": False, "auto.offset.reset": k.get("auto_offset_reset", "earliest"),
            "session.timeout.ms": int(k.get("session_timeout_ms", 45000)),
            "max.poll.interval.ms": int(k.get("max_poll_interval_ms", 1800000)),
            "topic.metadata.refresh.interval.ms": 10000, "allow.auto.create.topics": False,
            "client.id": "cdc-es-sync",
        })
        consumer.subscribe([k["topic_pattern"]],
                           on_assign=lambda c, p: log.info("assigned %s", [f"{x.topic}[{x.partition}]" for x in p]),
                           on_revoke=lambda c, p: log.info("revoked %d partitions", len(p)))
        self.metrics.beat("running")
        log.info("cdc-es-sync running")
        last_lag = 0.0
        try:
            while not self.stop.is_set():
                batch_cfg = self.cfg.get("batch", {})
                messages = self._collect(consumer, int(batch_cfg.get("max_events", 500)),
                                         float(batch_cfg.get("max_wait_seconds", 1.0)))
                self.metrics.beat()
                if messages:
                    self._handle(consumer, messages)
                if time.time() - last_lag > 15:
                    self._lag(consumer)
                    last_lag = time.time()
        except ShutdownRequested:
            log.info("shutdown requested; uncommitted events will be re-read on the next start")
        finally:
            consumer.close()
            self.producer.flush(5)
            self.reader.close()
            self.metrics.beat("stopped")
            log.info("stopped")

    def _collect(self, consumer, max_events: int, max_wait: float) -> list:
        messages, deadline = [], time.time() + max_wait
        while len(messages) < max_events and time.time() < deadline and not self.stop.is_set():
            batch = consumer.consume(num_messages=max_events - len(messages), timeout=min(0.25, max_wait))
            for msg in batch:
                if msg.error():
                    code = msg.error().code()
                    if code in (KafkaError._PARTITION_EOF, KafkaError.UNKNOWN_TOPIC_OR_PART):
                        continue
                    raise KafkaException(msg.error())
                messages.append(msg)
        return messages

    def _handle(self, consumer, messages: list):
        started = time.time()
        changes, high = [], {}
        for msg in messages:
            key = (msg.topic(), msg.partition())
            high[key] = max(high.get(key, -1), msg.offset())
            try:
                change = parse_event(msg.value(), msg.topic(), msg.partition(), msg.offset())
            except BadEvent as exc:
                self.metrics.skipped.labels("bad_event").inc()
                self.dlq.send("parse", str(exc), topic=msg.topic(), partition=msg.partition(), offset=msg.offset(),
                              value=(msg.value() or b"")[:2000].decode("utf-8", "replace"))
                continue
            if change is None:
                self.metrics.skipped.labels("no_row_change").inc()
            else:
                changes.append(change)
        if changes:
            self.engine.process(changes)
        consumer.commit(offsets=[TopicPartition(t, p, o + 1) for (t, p), o in high.items()], asynchronous=False)
        self.metrics.batches.inc()
        self.metrics.batch_seconds.observe(time.time() - started)
        self.metrics.last_batch.set(time.time())
        log.info("batch committed: %d messages, %d row changes, %.2fs", len(messages), len(changes), time.time() - started)

    def _lag(self, consumer):
        try:
            for tp in consumer.assignment():
                low, high = consumer.get_watermark_offsets(tp, timeout=5, cached=False)
                pos = consumer.position([tp])[0].offset
                lag = max(0, high - (pos if pos >= 0 else low))
                self.metrics.lag.labels(tp.topic, str(tp.partition)).set(lag)
        except Exception as exc:
            log.debug("lag check failed: %s", exc)

    def _ensure_dlq_topic(self):
        topic = self.cfg["kafka"]["dlq_topic"]
        admin = AdminClient({"bootstrap.servers": self.cfg["kafka"]["bootstrap_servers"]})
        if topic in admin.list_topics(timeout=10).topics:
            return
        for _, future in admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)]).items():
            try:
                future.result()
                log.info("created dead letter topic %s", topic)
            except Exception as exc:
                log.warning("could not create %s (%s); dead letters will go to files", topic, exc)
