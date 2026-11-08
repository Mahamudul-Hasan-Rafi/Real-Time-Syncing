"""Elasticsearch bulk retries and the dead letter fallback, with fake clients."""
import json

import elasticsearch as es
import pytest

from cdc_es_sync.connectors.dlq import DeadLetterQueue
from cdc_es_sync.connectors.elastic import BulkOp, ElasticsearchWriter, make_client
from cdc_es_sync.errors import ShutdownRequested
from cdc_es_sync.lifecycle import StopSignal
from cdc_es_sync.observability.metrics import Metrics
from cdc_es_sync.settings import ElasticsearchTarget

TARGET = ElasticsearchTarget(hosts=("http://localhost:9200",))


class FakeBulkClient:
    """Answers each bulk call with the next scripted result: an exception, or one status per item."""

    def __init__(self, *script):
        self.script, self.calls = list(script), []

    def bulk(self, operations, refresh):
        self.calls.append(operations)
        step = self.script.pop(0)
        if isinstance(step, Exception):
            raise step
        # the action lines of the body; the document lines in between carry no _id
        actions = [action for line in operations for action, meta in line.items()
                   if action in ("index", "update", "delete") and isinstance(meta, dict) and "_id" in meta]
        return {"items": [{a: {"status": s, "error": None if s < 300 else {"type": "x"}}} for a, s in zip(actions, step)]}


def writer(client):
    w = ElasticsearchWriter({"default": TARGET}, StopSignal(), Metrics(), max_backoff=0.001)
    w.clients["default"] = client
    return w


def ops(*actions):
    return [BulkOp(a, "idx", str(i), {"f": i} if a != "delete" else None) for i, a in enumerate(actions)]


def test_retryable_items_are_resent_alone_until_accepted():
    client = FakeBulkClient([201, 429], [200])
    assert writer(client).apply("default", ops("index", "index")) == []
    assert len(client.calls) == 2 and client.calls[1][0]["index"]["_id"] == "1"


def test_unreachable_cluster_is_retried():
    client = FakeBulkClient(es.ConnectionError("down"), [201])
    assert writer(client).apply("default", ops("index")) == []


def test_refused_document_is_reported_not_retried():
    failed = writer(FakeBulkClient([400])).apply("default", ops("index"))
    assert [op.doc_id for op, _ in failed] == ["0"]


def test_delete_of_a_missing_document_is_success():
    assert writer(FakeBulkClient([404])).apply("default", ops("delete")) == []


def test_stop_interrupts_the_retry_wait():
    w = writer(FakeBulkClient(es.ConnectionError("down")))
    w.stop.set()
    with pytest.raises(ShutdownRequested):
        w.apply("default", ops("index"))


def test_certificate_checks_can_be_switched_off():
    client = make_client(ElasticsearchTarget(hosts=("https://es:9200",), verify_certs=False, username="elastic"))
    assert client is not None


# ---------------------------------------------------------------- dead letters
class FakeProducer:
    def __init__(self, unflushed=0):
        self.sent, self.unflushed = [], unflushed

    def produce(self, topic, value):
        self.sent.append((topic, json.loads(value)))

    def flush(self, timeout):
        return self.unflushed


def test_dead_letter_goes_to_the_topic(tmp_path):
    producer = FakeProducer()
    DeadLetterQueue(producer, "dlq", tmp_path, Metrics()).send("parse", "bad", offset=3)
    assert producer.sent[0][0] == "dlq" and producer.sent[0][1]["offset"] == 3
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("producer", [None, FakeProducer(unflushed=1)], ids=["no producer", "flush timed out"])
def test_dead_letter_falls_back_to_a_file(tmp_path, producer):
    DeadLetterQueue(producer, "dlq", tmp_path, Metrics()).send("parse", "bad", offset=3)
    (file,) = tmp_path.glob("dlq-*.ndjson")
    record = json.loads(file.read_text(encoding="utf-8"))
    assert record["stage"] == "parse" and record["offset"] == 3
