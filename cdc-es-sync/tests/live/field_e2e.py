"""Live test of field-level updates. The service must be running (python -m cdc_es_sync run).

Changes REAL rows in the dev Oracle schema and checks, for every affected Elasticsearch document:
  * only the expected fields changed, to the expected values; every other field is byte-identical
    (including etl_loaded_at: nothing was rebuilt);
  * the document was written (_seq_no went up);
and for documents that must NOT be touched, that they were not written at all (_seq_no unchanged).
Then the change is reverted and every document must be identical to its original.
Every change registers its revert first, so a crash or Ctrl+C still restores the data (also run at exit).

    python tests/live/field_e2e.py [scenario name fragment ...]
"""
import atexit
import sys
import time
import traceback
from pathlib import Path
from zoneinfo import ZoneInfo

import elasticsearch as es
import oracledb

from cdc_es_sync.config import load_config
from cdc_es_sync.es_writer import make_client
from cdc_es_sync.mapping import Registry
from cdc_es_sync.oracle_reader import OracleReader  # noqa: F401  (sets fetch_lobs default)
from cdc_es_sync.transform import iso

ROOT = Path(__file__).resolve().parents[2]
cfg = load_config(ROOT / "config.yaml")
TZ = ZoneInfo(cfg["oracle"]["timezone"])
registry = Registry.from_config(cfg)
client = make_client(cfg["elasticsearch"]["targets"]["default"], request_timeout=300)
ora = cfg["oracle"]
conn = oracledb.connect(user=ora["user"], password=ora["password"], dsn=ora["dsn"])
ONLY = [a.lower() for a in sys.argv[1:]]
CUSTOMER, BALANCE, TRAN = "customer_data", "ac_balance_data", "transaction_history_"
CAP = 6000                 # most documents one group may hold
SETTLE = 12                # seconds to wait before checking that untouched documents were not written
RESULTS, REVERTS = [], []


# ----------------------------------------------------------------------------------------------- helpers
def q(sql, **b):
    with conn.cursor() as cur:
        cur.execute(sql, b)
        return cur.fetchall()


def dml(sql, **b):
    with conn.cursor() as cur:
        cur.execute(sql, b)
    conn.commit()


def run_reverts():
    while REVERTS:
        desc, fn = REVERTS.pop()
        try:
            fn()
            print(f"  [revert] {desc}", flush=True)
        except Exception as exc:
            print(f"  [REVERT FAILED] {desc}: {exc}", flush=True)


atexit.register(run_reverts)


class TooBig(Exception):
    pass


_MAPPINGS = {}


def exact(index, field):
    """Field name usable in a term query: text fields are matched on their .keyword sub-field."""
    if index not in _MAPPINGS:
        _MAPPINGS[index] = client.indices.get_mapping(index=index)[index]["mappings"]["properties"]
    p = _MAPPINGS[index].get(field, {})
    return f"{field}.keyword" if p.get("type") == "text" and "keyword" in (p.get("fields") or {}) else field


def term(index, field, value):
    return {"term": {exact(index, field): value}}


def find_ids(index, query):
    client.indices.refresh(index=index)
    hits = client.search(index=index, query=query, size=CAP + 1, source=False)["hits"]["hits"]
    if len(hits) > CAP:
        raise TooBig()
    return [h["_id"] for h in hits]


def snapshot(index, ids):
    out = {}
    for i in range(0, len(ids), 1000):
        for d in client.mget(index=index, ids=ids[i:i + 1000])["docs"]:
            if d.get("found"):
                out[d["_id"]] = (d["_source"], d["_seq_no"])
    return out


def wait_until(fn, timeout, interval=1.5):
    start = time.time()
    while time.time() - start < timeout:
        try:
            if fn():
                return time.time() - start
        except es.NotFoundError:
            pass
        time.sleep(interval)
    return None


def mark(old):
    s = str(old)
    return s[:-1] + ("#" if s[-1] != "#" else "$")


def selected(name):
    return not ONLY or any(o in name.lower() for o in ONLY)


def record(name, ok, detail=""):
    RESULTS.append((name, ok, detail))
    print(f"[{'PASS' if ok else 'FAIL'}] {name} {detail}", flush=True)


class Group:
    """Documents of one index selected by a query. `fields` maps field -> function(old value) -> expected new value;
    no fields = the documents must not be written at all."""

    def __init__(self, index, query, fields=None, optional=False):
        self.index, self.query, self.fields, self.optional = index, query, fields or {}, optional
        self.before = {}

    @property
    def touched(self):
        return bool(self.fields)


def scenario(name, table, changes, candidates, groups_of):
    """changes: {column: function(old) -> new value (None function = set NULL)}.
    candidates: SQL returning rowid, then the old value of each changed column, then what groups_of needs.
    groups_of(row) -> [Group]."""
    if not selected(name):
        return
    print(f"\n== {name}", flush=True)
    cols = list(changes)
    for row in q(candidates):
        rowid, olds = row[0], dict(zip(cols, row[1:1 + len(cols)]))
        if any(v in (None, "") for v in olds.values()):
            continue
        groups = groups_of(row)
        try:
            for g in groups:
                g.before = snapshot(g.index, find_ids(g.index, g.query))
        except TooBig:
            continue
        # Every group must hold documents, or its check would prove nothing (optional groups may be empty).
        if not any(g.touched and g.before for g in groups) or not all(g.before for g in groups if not g.optional):
            continue
        news = {c: (None if fn is None else fn(olds[c])) for c, fn in changes.items()}

        def restore(rowid=rowid, olds=olds):
            sets = ", ".join(f"{c} = :{c}" for c in olds)
            dml(f"UPDATE emob.{table} SET {sets} WHERE rowid = :r", r=rowid, **olds)
        REVERTS.append((f"{table} {rowid} back to its original values", restore))
        try:
            sets = ", ".join(f"{c} = :{c}" for c in news)
            dml(f"UPDATE emob.{table} SET {sets} WHERE rowid = :r", r=rowid, **news)
        except oracledb.DatabaseError as exc:
            REVERTS.pop()
            record(name, False, f"database refused the test value: {str(exc).splitlines()[0]}")
            return
        total = sum(len(g.before) for g in groups if g.touched)

        def expected_doc(g, doc_id):
            src = g.before[doc_id][0]
            return {**src, **{f: fn(src.get(f)) for f, fn in g.fields.items()}}

        def wrong_docs(g):
            now = snapshot(g.index, list(g.before))
            return [(i, now.get(i, ({},))[0]) for i in g.before if i not in now or now[i][0] != expected_doc(g, i)]

        t1 = wait_until(lambda: not any(wrong_docs(g) for g in groups if g.touched), 120 + total / 40)
        problems = []
        if t1 is None:
            for g in groups:
                bad = wrong_docs(g) if g.touched else []
                if bad:
                    i, got = bad[0]
                    exp = expected_doc(g, i)
                    diff = [f"{k}: expected={str(exp.get(k))[:60]} got={str(got.get(k))[:60]}"
                            for k in sorted(set(exp) | set(got)) if exp.get(k) != got.get(k)]
                    problems.append(f"{g.index}: {len(bad)}/{len(g.before)} documents wrong, e.g. {i}: {diff[:3]}")
        else:
            time.sleep(SETTLE)
            for g in groups:
                now = snapshot(g.index, list(g.before))
                if g.touched:
                    unwritten = [i for i in g.before if now[i][1] <= g.before[i][1]]
                    if unwritten:
                        problems.append(f"{g.index}: {len(unwritten)} documents were not written")
                else:
                    written = [i for i in g.before if now.get(i, (None, -1))[1] != g.before[i][1]]
                    if written:
                        problems.append(f"{g.index}: {len(written)} of {len(g.before)} documents were written but must not be")
        restore()
        REVERTS.pop()

        def reverted():
            for g in groups:
                now = snapshot(g.index, list(g.before))
                if any(i not in now or now[i][0] != g.before[i][0] for i in g.before):
                    return False
            return True
        t2 = wait_until(reverted, 120 + total / 40)
        if t1 is None:
            record(name, False, "; ".join(problems) or "documents did not take the new values")
        elif problems:
            record(name, False, "; ".join(problems))
        elif t2 is None:
            record(name, False, "documents did not return to their original content after the revert")
        else:
            counts = ", ".join(f"{g.index}={len(g.before)}{'' if g.touched else ' untouched'}" for g in groups)
            record(name, True, f"{counts} apply={t1:.0f}s revert={t2:.0f}s")
        return
    record(name, False, "no suitable row found (documents missing or fan-out above the cap)")


def customer_groups(touch):
    """touch: {index: {field: fn}}; indices not listed are watched as 'must not be written'."""
    def make(row):
        cust_no = row[-1]
        return [Group(index, term(index, "cust_no", cust_no), touch.get(index)) for index in (CUSTOMER, BALANCE, TRAN)]
    return make


# ----------------------------------------------------------------------------------------------- scenarios
def main():
    pick = ("SELECT rowid, {cols}, cust_no FROM emob.mb_customer_mst WHERE {cond} "
            "AND cust_no IN (SELECT a.cust_no FROM emob.mb_account_mst a JOIN emob.mb_transaction_dtl t ON t.ac_id = a.ac_id "
            "WHERE ROWNUM <= 30000) AND ROWNUM <= 200")

    # the case in question
    scenario("customer changes mobile_no", "mb_customer_mst", {"mobile_no": mark},
             pick.format(cols="mobile_no", cond="mobile_no IS NOT NULL"),
             customer_groups({CUSTOMER: {"mobile_no": mark}, TRAN: {"mobile_no": mark}}))

    scenario("customer changes mobile_no2 (only the transaction index carries it)", "mb_customer_mst", {"mobile_no2": mark},
             pick.format(cols="mobile_no2", cond="mobile_no2 IS NOT NULL"),
             customer_groups({TRAN: {"mobile_no2": mark}}))

    scenario("customer clears mobile_no2 (set to NULL)", "mb_customer_mst", {"mobile_no2": None},
             pick.format(cols="mobile_no2", cond="mobile_no2 IS NOT NULL"),
             customer_groups({TRAN: {"mobile_no2": lambda old: None}}))

    scenario("customer changes mobile_no and mobile_no2 in one statement", "mb_customer_mst",
             {"mobile_no": mark, "mobile_no2": mark},
             pick.format(cols="mobile_no, mobile_no2", cond="mobile_no IS NOT NULL AND mobile_no2 IS NOT NULL"),
             customer_groups({CUSTOMER: {"mobile_no": mark}, TRAN: {"mobile_no": mark, "mobile_no2": mark}}))

    scenario("customer changes first name (not an agent or operator): customer index only", "mb_customer_mst", {"first_name": mark},
             pick.format(cols="first_name", cond="first_name IS NOT NULL AND cust_no NOT IN "
                         "(SELECT agent_id FROM emob.st_agent_point WHERE agent_id IS NOT NULL) AND cust_no NOT IN "
                         "(SELECT cust_no FROM emob.mb_account_operator)"),
             customer_groups({CUSTOMER: {"first_name": mark}}))

    scenario("account changes title: all three indices", "mb_account_mst", {"ac_title": mark},
             "SELECT rowid, ac_title, ac_id FROM emob.mb_account_mst SAMPLE (1) WHERE ac_title IS NOT NULL AND ROWNUM <= 80",
             lambda row: [Group(i, term(i, "ac_id", row[-1]), {"ac_title": mark}) for i in (CUSTOMER, BALANCE, TRAN)])

    scenario("transaction changes narration: that document only", "mb_transaction_dtl", {"naration": mark},
             "SELECT rowid, naration, tran_id FROM emob.mb_transaction_dtl SAMPLE (0.05) "
             "WHERE naration IS NOT NULL AND LENGTH(naration) > 2 AND ROWNUM <= 80",
             lambda row: [Group(TRAN, {"ids": {"values": [str(row[-1])]}}, {"naration": mark})])

    bump = lambda old: float(old) + 1234.5          # noqa: E731
    scenario("balance changes amount: that document only", "mb_account_daily_balance", {"balance": bump},
             "SELECT rowid, balance, ac_id, value_date FROM emob.mb_account_daily_balance SAMPLE (0.05) "
             "WHERE balance IS NOT NULL AND ROWNUM <= 80",
             lambda row: [Group(BALANCE, {"ids": {"values": [f"{row[2]}_{iso(row[3], TZ)}"]}}, {"balance": bump})])

    scenario("product changes description: three indices", "mb_product_mst", {"full_desc": mark},
             "SELECT rowid, full_desc, ac_type_id FROM emob.mb_product_mst p WHERE full_desc IS NOT NULL AND "
             "(SELECT COUNT(*) FROM emob.mb_product_mst p2 WHERE p2.full_desc = p.full_desc) = 1 "
             "ORDER BY (SELECT COUNT(*) FROM emob.mb_account_mst a WHERE a.ac_type_id = p.ac_type_id)",
             lambda row: [Group(CUSTOMER, term(CUSTOMER, "ac_type_id", row[-1]), {"full_desc": mark}),
                          Group(BALANCE, term(BALANCE, "product", row[1]), {"product": mark}),
                          Group(TRAN, term(TRAN, "ac_type_id", row[-1]), {"product_name": mark})])

    # A transaction document can carry the point twice: where the transaction happened and the account's point.
    def point_groups(row):
        p = row[-1]
        at, acct = term(TRAN, "tran_point_id", p), term(TRAN, "ac_point_id", p)
        return [Group(TRAN, {"bool": {"must": [at], "must_not": [acct]}}, {"point_name": mark}, optional=True),
                Group(TRAN, {"bool": {"must": [acct], "must_not": [at]}}, {"ac_point_name": mark}, optional=True),
                Group(TRAN, {"bool": {"must": [at, acct]}}, {"point_name": mark, "ac_point_name": mark}, optional=True),
                Group(CUSTOMER, {"bool": {"must": [term(CUSTOMER, "ac_point_id", p), term(CUSTOMER, "agent_point_id", p)]}},
                      {"point_name": mark}, optional=True),
                Group(BALANCE, term(BALANCE, "point_name", row[1]), {"point_name": mark})]
    scenario("agent point changes name: account point and transaction point fields", "st_agent_point", {"point_name": mark},
             "SELECT rowid, point_name, point_id FROM emob.st_agent_point p WHERE point_name IS NOT NULL AND "
             "(SELECT COUNT(*) FROM emob.st_agent_point p2 WHERE p2.point_name = p.point_name) = 1 "
             "AND point_id IN (SELECT point_id FROM emob.mb_transaction_dtl) "
             "ORDER BY (SELECT COUNT(*) FROM emob.mb_transaction_dtl t WHERE t.tran_point_id = p.point_id)",
             point_groups)

    scenario("transaction code changes description: transaction index only", "st_transaction_code", {"tran_desc": mark},
             "SELECT rowid, tran_desc, tran_code FROM emob.st_transaction_code c WHERE tran_desc IS NOT NULL "
             "ORDER BY (SELECT COUNT(*) FROM emob.mb_transaction_dtl t WHERE t.tran_code = c.tran_code)",
             lambda row: [Group(TRAN, term(TRAN, "tran_code", row[-1]), {"tran_desc": mark})])

    scenario("code table changes occupation text: three indices", "stcodmas", {"coddes": mark},
             "SELECT rowid, coddes, sofcod FROM emob.stcodmas s WHERE harcod = 'OCC' AND coddes IS NOT NULL AND "
             "(SELECT COUNT(*) FROM emob.stcodmas s2 WHERE s2.coddes = s.coddes) = 1 "
             "ORDER BY (SELECT COUNT(*) FROM emob.mb_customer_mst c WHERE c.occupation_code = s.sofcod)",
             lambda row: [Group(i, term(i, "occupation", row[1]), {"occupation": mark}) for i in (CUSTOMER, BALANCE, TRAN)])

    untouched_scenario()


def unmapped_customer_column():
    """A VARCHAR2 column of MB_CUSTOMER_MST that no index reads (so changing it must write nothing)."""
    mapped = {c for i in registry.tables["MB_CUSTOMER_MST"] for c in i.columns}
    rows = q("SELECT column_name FROM all_tab_columns WHERE owner = 'EMOB' AND table_name = 'MB_CUSTOMER_MST' "
             "AND data_type = 'VARCHAR2' AND data_length >= 5 ORDER BY column_id")
    for (col,) in rows:
        if col not in mapped and q(f"SELECT COUNT(*) FROM (SELECT 1 FROM emob.mb_customer_mst WHERE {col} IS NOT NULL "
                                   f"AND ROWNUM <= 50)")[0][0] >= 20:
            return col
    return None


def untouched_scenario():
    """The no-op case: a column nobody reads changes, so no document may be written."""
    col = unmapped_customer_column()
    name = f"customer changes {col.lower() if col else '?'} (no index reads it): nothing is written"
    if not selected(name):
        return
    print(f"\n== {name}", flush=True)
    if not col:
        record(name, False, "every VARCHAR2 column of MB_CUSTOMER_MST is mapped; nothing to test with")
        return
    for rowid, old, cust_no in q(f"SELECT rowid, {col}, cust_no FROM emob.mb_customer_mst "
                                 f"WHERE {col} IS NOT NULL AND cust_no IN (SELECT a.cust_no FROM emob.mb_account_mst a JOIN "
                                 f"emob.mb_transaction_dtl t ON t.ac_id = a.ac_id WHERE ROWNUM <= 30000) AND ROWNUM <= 100"):
        groups = [Group(i, term(i, "cust_no", cust_no)) for i in (CUSTOMER, BALANCE, TRAN)]
        try:
            for g in groups:
                g.before = snapshot(g.index, find_ids(g.index, g.query))
        except TooBig:
            continue
        if not all(g.before for g in groups[:2]):
            continue

        def restore(rowid=rowid, old=old):
            dml(f"UPDATE emob.mb_customer_mst SET {col} = :v WHERE rowid = :r", v=old, r=rowid)
        REVERTS.append(("unmapped column back", restore))
        dml(f"UPDATE emob.mb_customer_mst SET {col} = :v WHERE rowid = :r", v=mark(old), r=rowid)
        time.sleep(SETTLE + 15)         # long enough for Debezium to publish and the service to act
        restore()
        REVERTS.pop()
        time.sleep(SETTLE + 10)
        written = {}
        for g in groups:
            now = snapshot(g.index, list(g.before))
            written[g.index] = sum(1 for i in g.before if now.get(i, (0, -1))[1] != g.before[i][1])
        record(name, not any(written.values()), f"column {col}; documents written: {written}")
        return
    record(name, False, "no suitable row found")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        RESULTS.append(("harness", False, "exception, see above"))
    finally:
        run_reverts()
    print("\n==================== SUMMARY")
    for name, ok, detail in RESULTS:
        print(f"{'PASS' if ok else 'FAIL'}  {name}  {detail}")
    sys.exit(0 if RESULTS and all(r[1] for r in RESULTS) else 1)
