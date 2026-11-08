"""Kafka clients: the change-event consumer, the dead letter producer and topic administration."""
from __future__ import annotations

import logging

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from ..settings import KafkaSettings

log = logging.getLogger(__name__)

CLIENT_ID = "cdc-es-sync"


def consumer_config(kafka: KafkaSettings) -> dict[str, object]:
    """Offsets are committed by hand, only after Elasticsearch accepted a batch (at-least-once)."""
    return {
        "bootstrap.servers": kafka.bootstrap_servers,
        "group.id": kafka.group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": kafka.auto_offset_reset,
        "session.timeout.ms": kafka.session_timeout_ms,
        "max.poll.interval.ms": kafka.max_poll_interval_ms,
        "topic.metadata.refresh.interval.ms": 10000,
        "allow.auto.create.topics": False,
        "client.id": CLIENT_ID,
    }


def create_consumer(kafka: KafkaSettings) -> Consumer:
    return Consumer(consumer_config(kafka))


def create_producer(kafka: KafkaSettings) -> Producer:
    return Producer({"bootstrap.servers": kafka.bootstrap_servers, "client.id": f"{CLIENT_ID}-dlq"})


def topic_count(kafka: KafkaSettings, timeout: float = 10) -> int:
    return len(AdminClient({"bootstrap.servers": kafka.bootstrap_servers}).list_topics(timeout=timeout).topics)


def ensure_topic(kafka: KafkaSettings, topic: str) -> None:
    """Creates the topic if it does not exist. A failure is logged, not raised: dead letters then go to files."""
    admin = AdminClient({"bootstrap.servers": kafka.bootstrap_servers})
    if topic in admin.list_topics(timeout=10).topics:
        return
    for _, future in admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)]).items():
        try:
            future.result()
            log.info("created dead letter topic %s", topic)
        except Exception as exc:
            log.warning("could not create %s (%s); dead letters will go to files", topic, exc)


def partition_lag(low: int, high: int, position: int, committed: int) -> int:
    """Events not yet consumed. `position` is unknown (negative) until the first message of a partition arrives after
    an assignment; then the group's committed offset is the truth, and only a group that never committed starts at
    `low`."""
    start = position if position >= 0 else committed if committed >= 0 else low
    return max(0, high - start)
