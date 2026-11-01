from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from cdc_es_sync.transform import RejectError, build_document, iso, key_doc_id

TZ = ZoneInfo("Asia/Dhaka")


def test_oracle_date_becomes_utc_iso_like_logstash():
    # 2020-11-12 00:00 in Dhaka is 2020-11-11T18:00:00.000Z, the value in the existing index.
    assert iso(datetime(2020, 11, 12), TZ) == "2020-11-11T18:00:00.000Z"
    assert iso(datetime(2014, 6, 22, 11, 10, 24), TZ) == "2014-06-22T05:10:24.000Z"


def test_balance_document_and_id(registry):
    ix = registry.indices["ac_balance_data"]
    row = {"ac_id": 3124, "balance": 53059.59, "balance_date": datetime(2020, 11, 12), "ac_no": "1083475000008",
           "gender": None, "create_date": datetime(2014, 6, 22, 11, 10, 24)}
    doc_id, doc = build_document(ix, row, TZ, "+06:00", "2026-01-01T00:00:00.000000Z")
    assert doc_id == "3124_2020-11-11T18:00:00.000Z"          # same id the Logstash load wrote
    assert doc["balance_date"] == "2020-11-11T18:00:00.000Z"
    assert doc["@timestamp"] == "2020-11-11T18:00:00.000Z"    # business date, as in the pipelines
    assert doc["etl_pipeline"] == "cdc_ac_balance_data"
    assert "gender" in doc and doc["gender"] is None            # empty columns stay null, as in the Logstash load
    assert doc["balance"] == 53059.59 and doc["ac_id"] == 3124


def test_key_doc_id_for_deleted_balance_row(registry):
    ix = registry.indices["ac_balance_data"]
    assert key_doc_id(ix, (3124, datetime(2020, 11, 12)), TZ) == "3124_2020-11-11T18:00:00.000Z"


def test_customer_json_and_nested_dates(registry):
    ix = registry.indices["customer_data"]
    row = {"ac_no": "A1", "create_date": datetime(2014, 6, 22, 11, 10, 24),
           "doc": '[{"document_id":1,"issue_date":"2020-01-02T03:04:05","expire_date":null}]',
           "nominee": '[{"nominee_id":7,"dob":"1990-05-05T00:00:00"}]',
           "address": None, "operator": '[{"ac_id":1,"inactive_date":null,"create_date":"2020-02-02T00:00:00"}]'}
    doc_id, doc = build_document(ix, row, TZ, "+06:00")
    assert doc_id == "A1"
    assert doc["doc"][0]["issue_date"] == "2020-01-02T03:04:05+06:00"
    assert doc["doc"][0]["expire_date"] is None                     # null stays null, no offset
    assert doc["nominee"][0]["dob"] == "1990-05-05T00:00:00+06:00"
    assert doc["operator"][0]["create_date"] == "2020-02-02T00:00:00+06:00"
    assert doc["address"] is None


def test_null_key_is_rejected_not_written_as_None(registry):
    with pytest.raises(RejectError):
        build_document(registry.indices["customer_data"], {"ac_no": None}, TZ, "+06:00")


def test_invalid_json_is_rejected(registry):
    with pytest.raises(RejectError):
        build_document(registry.indices["customer_data"], {"ac_no": "A1", "doc": "[{broken"}, TZ, "+06:00")


def test_missing_key_is_rejected(registry):
    with pytest.raises(RejectError):
        build_document(registry.indices["transaction_history"], {"amount": 5}, TZ, "+06:00")


def test_decimal_and_bytes(registry):
    ix = registry.indices["transaction_history"]
    _, doc = build_document(ix, {"tran_id": 1, "amount": Decimal("12.50"), "whole": Decimal("7")}, TZ, "+06:00")
    assert doc["amount"] == 12.5 and doc["whole"] == 7
