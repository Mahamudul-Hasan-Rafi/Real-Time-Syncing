"""The reconcile command: counts and totals, with fake Oracle and Elasticsearch."""
from cdc_es_sync.reconcile import format_report, reconcile


class FakeCursor:
    def __init__(self, answers):
        self.answers, self.sql = answers, []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, *a):
        self.sql.append(sql)
        self.last = sql

    def fetchone(self):
        # the answer is looked up by the amount column named in the SELECT
        for field, answer in self.answers.items():
            if f"SUM({field})" in self.last:
                return answer
        raise AssertionError(self.last)


class FakeReader:
    def __init__(self, answers):
        self.c = FakeCursor(answers)

    def cursor(self):
        return self.c


class FakeEs:
    def __init__(self, counts):
        self.counts, self.indices = counts, self

    def refresh(self, index):
        pass

    def search(self, index, body):
        rows, total = self.counts[index]
        return {"hits": {"total": {"value": rows}}, "aggregations": {"total": {"value": total}}}


class FakeWriter:
    def __init__(self, counts):
        self.es = FakeEs(counts)

    def client(self, target):
        return self.es


ORACLE = {"initial_amt": (10, 100.0), "balance": (20, 2000.5), "amount": (30, 3000.0)}


def counts(**changes):
    base = {"customer_data": (10, 100.0), "ac_balance_data": (20, 2000.5), "transaction_history_": (30, 3000.0)}
    base.update(changes)
    return base


def test_matching_counts_and_totals_pass(registry):
    rows, ok = reconcile(registry, FakeReader(ORACLE), FakeWriter(counts()))
    assert ok and [r["status"] for r in rows] == ["OK"] * 3


def test_total_within_one_currency_unit_passes(registry):
    # Elasticsearch sums in floating point; the old script allowed the same tolerance
    rows, ok = reconcile(registry, FakeReader(ORACLE), FakeWriter(counts(ac_balance_data=(20, 2000.0))))
    assert ok


def test_missing_document_is_a_mismatch(registry):
    rows, ok = reconcile(registry, FakeReader(ORACLE), FakeWriter(counts(transaction_history_=(29, 3000.0))))
    assert not ok
    assert [r["status"] for r in rows] == ["OK", "OK", "MISMATCH"]


def test_wrong_total_is_a_mismatch(registry):
    rows, ok = reconcile(registry, FakeReader(ORACLE), FakeWriter(counts(customer_data=(10, 90.0))))
    assert not ok and rows[0]["status"] == "MISMATCH"


def test_query_uses_the_document_query_restricted_to_the_amount_column(registry):
    reader = FakeReader(ORACLE)
    reconcile(registry, reader, FakeWriter(counts()))
    sql = reader.c.sql[1]               # ac_balance_data
    assert sql.startswith("SELECT COUNT(*), SUM(balance) FROM (SELECT B.BALANCE")
    assert "mb_account_daily_balance" in sql.lower() and "a.ac_id = b.ac_id" in sql.lower()


def test_report_lists_every_index(registry):
    rows, _ = reconcile(registry, FakeReader(ORACLE), FakeWriter(counts()))
    text = format_report(rows)
    assert all(name in text for name in ("customer_data", "ac_balance_data", "transaction_history_"))
