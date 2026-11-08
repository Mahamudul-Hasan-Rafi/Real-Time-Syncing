"""Elasticsearch bulk retries and the dead letter fallback, with fake clients."""
import json

import elasticsearch as es
import pytest
from elastic_transport import ApiResponseMeta, HttpHeaders, NodeConfig

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


def writer(client, metrics=None):
    w = ElasticsearchWriter({"default": TARGET}, StopSignal(), metrics or Metrics(), max_backoff=0.001)
    w.clients["default"] = client
    return w


def api_error(cls, status):
    """An elasticsearch ApiError as the client raises it for an HTTP status."""
    meta = ApiResponseMeta(status=status, http_version="1.1", headers=HttpHeaders(), duration=0.0,
                           node=NodeConfig("http", "localhost", 9200))
    return cls(message=f"HTTP {status}", meta=meta, body={})


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


# A refused login or a read-only index is not a poison event: the events must wait, not be dead-lettered and committed.
@pytest.mark.parametrize("error", [api_error(es.AuthenticationException, 401), api_error(es.AuthorizationException, 403)],
                         ids=["401 login", "403 permission"])
def test_refused_access_is_retried_not_raised(error):
    client = FakeBulkClient(error, [201])
    assert writer(client).apply("default", ops("index")) == []
    assert len(client.calls) == 2


def test_items_blocked_with_403_are_resent_not_dead_lettered():
    """403 on an item is how Elasticsearch reports a read-only index (disk flood stage) or a missing permission."""
    client = FakeBulkClient([201, 403], [200])
    assert writer(client).apply("default", ops("index", "index")) == []
    assert len(client.calls) == 2 and client.calls[1][0]["index"]["_id"] == "1"


def test_stop_interrupts_the_wait_for_access():
    w = writer(FakeBulkClient(api_error(es.AuthenticationException, 401)))
    w.stop.set()
    with pytest.raises(ShutdownRequested):
        w.apply("default", ops("index"))


def test_other_client_errors_still_raise_so_a_poison_batch_can_be_split():
    with pytest.raises(es.BadRequestError):
        writer(FakeBulkClient(api_error(es.BadRequestError, 400))).apply("default", ops("index"))


@pytest.mark.parametrize("script", [[es.ConnectionError("down"), [201]], [api_error(es.ApiError, 503), [201]]],
                         ids=["unreachable", "503"])
def test_waiting_for_a_cluster_that_is_down_keeps_the_heartbeat(script):
    metrics, beats = Metrics(), []
    metrics.beat = lambda state=None: beats.append(state)
    writer(FakeBulkClient(*script), metrics).apply("default", ops("index"))
    assert len(beats) == 1


def test_being_refused_does_not_keep_the_heartbeat_and_is_logged_as_an_error(caplog):
    """/health must turn 503 when the login is refused, or nobody would notice the service is stuck."""
    metrics, beats = Metrics(), []
    metrics.beat = lambda state=None: beats.append(state)
    with caplog.at_level("WARNING"):
        writer(FakeBulkClient(api_error(es.AuthenticationException, 401), [201]), metrics).apply("default", ops("index"))
    assert beats == []
    assert [r.levelname for r in caplog.records] == ["ERROR"] and "refused access" in caplog.records[0].getMessage()


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
