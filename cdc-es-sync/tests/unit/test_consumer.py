"""The consume loop: offsets are committed only after a batch was applied (at-least-once)."""
import json
from unittest.mock import patch

import pytest

from cdc_es_sync.errors import ShutdownRequested
from cdc_es_sync.lifecycle import StopSignal
from cdc_es_sync.observability.metrics import Metrics
from cdc_es_sync.pipeline.consumer import ChangeConsumer
from cdc_es_sync.settings import BatchSettings


class FakeMessage:
    def __init__(self, value, topic="emob.EMOB.MB_TRANSACTION_DTL", partition=0, offset=0):
        self._value, self._topic, self._partition, self._offset = value, topic, partition, offset

    def value(self):
        return self._value

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def error(self):
        return None


class FakeKafka:
    def __init__(self):
        self.commits = []
        self.partitions = [object()]

    def commit(self, offsets, asynchronous):
        assert asynchronous is False
        self.commits.append(sorted((tp.topic, tp.partition, tp.offset) for tp in offsets))

    def assignment(self):
        return self.partitions


class FakeEngine:
    def __init__(self, fail=None):
        self.batches, self.fail = [], fail

    def process(self, changes):
        if self.fail:
            raise self.fail
        self.batches.append(changes)


class FakeDlq:
    def __init__(self):
        self.items = []

    def send(self, stage, reason, **detail):
        self.items.append((stage, reason))


def update(tran_id, **kw):
    payload = {"op": "u", "before": {"TRAN_ID": tran_id}, "after": {"TRAN_ID": tran_id}, "source": {"table": "X"}}
    return FakeMessage(json.dumps({"payload": payload}).encode(), **kw)


def loop(engine=None):
    kafka, dlq = FakeKafka(), FakeDlq()
    consumer = ChangeConsumer(kafka, engine or FakeEngine(), dlq, Metrics(), StopSignal(), BatchSettings())
    return consumer, kafka, dlq


def test_commits_the_next_offset_of_every_partition_after_applying():
    consumer, kafka, _ = loop()
    consumer.handle([update(1, offset=4), update(2, offset=7), update(3, partition=1, offset=2)])
    assert len(consumer.engine.batches[0]) == 3
    assert kafka.commits == [[("emob.EMOB.MB_TRANSACTION_DTL", 0, 8), ("emob.EMOB.MB_TRANSACTION_DTL", 1, 3)]]


def test_nothing_is_committed_when_applying_fails():
    consumer, kafka, _ = loop(FakeEngine(fail=ShutdownRequested()))
    with pytest.raises(ShutdownRequested):
        consumer.handle([update(1, offset=4)])
    assert kafka.commits == []


def test_a_malformed_event_goes_to_the_dlq_and_the_batch_is_still_committed():
    consumer, kafka, dlq = loop()
    consumer.handle([FakeMessage(b"not json", offset=1), update(2, offset=2)])
    assert [stage for stage, _ in dlq.items] == ["parse"]
    assert len(consumer.engine.batches[0]) == 1 and kafka.commits[0][0][2] == 3


def test_tombstones_are_committed_without_calling_the_engine():
    consumer, kafka, _ = loop()
    consumer.handle([FakeMessage(None, offset=5)])
    assert consumer.engine.batches == [] and kafka.commits[0][0][2] == 6


def test_assignment_watchdog_allows_an_assigned_consumer():
    consumer, _, _ = loop()
    assert consumer._check_assignment(10.0) is None


def test_assignment_watchdog_fails_a_persistently_detached_consumer():
    consumer, kafka, _ = loop()
    kafka.partitions = []
    with patch("cdc_es_sync.pipeline.consumer.time.monotonic", return_value=131.0):
        with pytest.raises(RuntimeError, match="no partition assignment"):
            consumer._check_assignment(10.0)
    assert consumer.metrics.health(120)[1]["state"] == "failed"
