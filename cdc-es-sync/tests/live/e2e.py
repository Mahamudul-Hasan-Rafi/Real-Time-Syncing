"""Live end-to-end test. The service must be running (python -m cdc_es_sync run).

Makes REAL changes in the dev Oracle schema, checks the right Elasticsearch documents change, then reverts the
change and checks every document is back to what it was. Every change registers its own revert first, so a crash
or Ctrl+C still restores the data (reverts also run at exit).

    python tests/live/e2e.py [scenario name fragment ...]
"""
import atexit
import random
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable
from zoneinfo import ZoneInfo

import elasticsearch as es
import oracledb
from elasticsearch import helpers

from cdc_es_sync.config import load_config
from cdc_es_sync.oracle_reader import OracleReader  # noqa: F401  (sets fetch_lobs default)
from cdc_es_sync.transform import iso

ROOT = Path(__file__).resolve().parents[2]
cfg = load_config(ROOT / "config.yaml")
TZ = ZoneInfo(cfg["oracle"]["timezone"])
client = es.Elasticsearch(cfg["elasticsearch"]["targets"]["default"]["hosts"], request_timeout=300)
_count = client.count


def _fresh_count(index=None, **kw):
    """Index refresh interval is 30s, so count is only exact after a refresh."""
    client.indices.refresh(index=index)
    return _count(index=index, **kw)


client.count = _fresh_count
ora = cfg["oracle"]
conn = oracledb.connect(user=ora["user"], password=ora["password"], dsn=ora["dsn"])
ONLY = [a.lower() for a in sys.argv[1:]]
MISSING = object()
# etl_*: lineage fields change on purpose when CDC rebuilds a document.
# age: the balance query computes it from SYSDATE, so a rebuild after a birthday correctly shows a new value.
IGNORE = {"etl_loaded_at", "etl_pipeline", "age"}
CUSTOMER, BALANCE, TRAN = "customer_data", "ac_balance_data", "transaction_history_"
OPERATIONS = set(cfg["sync"].get("operations") or "cudr")      # change types the service applies

RESULTS = []
REVERTS = []


# ----------------------------------------------------------------------------------------------- helpers
def q(sql, **b):
    with conn.cursor() as cur:
        cur.execute(sql, b)
        return cur.fetchall()


def dml(sql, **b):
    with conn.cursor() as cur:
        cur.execute(sql, b)
        n = cur.rowcount
    conn.commit()
    return n


def run_reverts():
    while REVERTS:
        desc, fn = REVERTS.pop()
        try:
            fn()
            print(f"  [revert] {desc}")
        except Exception as exc:
            print(f"  [REVERT FAILED] {desc}: {exc}")


atexit.register(run_reverts)


class TooBig(Exception):
    pass


def es_docs(index, query, limit=30000):
    # Search only sees what was refreshed (30 s interval). Without this, a snapshot taken right after the
    # previous scenario can still show that scenario's temporary value.
    client.indices.refresh(index=index)
    out = {}
    for h in helpers.scan(client, index=index, query={"query": query}, size=2000):
        out[h["_id"]] = h["_source"]
        if len(out) > limit:
            raise TooBig()
    return out


def mget(index, ids):
    out = {}
    ids = list(ids)
    for i in range(0, len(ids), 1000):
        res = client.mget(index=index, ids=ids[i:i + 1000])
        for d in res["docs"]:
            out[d["_id"]] = d["_source"] if d.get("found") else None
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


def strip(doc):
    return {k: v for k, v in doc.items() if k not in IGNORE} if doc else doc


def same(a, b):
    if isinstance(a, float) or isinstance(b, float):
        try:
            return abs(float(a) - float(b)) < 1e-6
        except (TypeError, ValueError):
            return False
    return a == b


def keyword(index, field):
    """term-query field name: text fields are matched on their .keyword sub-field."""
    return f"{field}.keyword" if (index, field) in TEXT_FIELDS else field


TEXT_FIELDS = {(CUSTOMER, "first_name"), (CUSTOMER, "ac_title"), (CUSTOMER, "point_name"), (CUSTOMER, "full_desc"),
               (BALANCE, "ac_title"), (BALANCE, "point_name"), (TRAN, "naration"),
               (CUSTOMER, "nominee.nominee_name"), (CUSTOMER, "address.village")}


@dataclass
class Check:
    index: str
    query: dict
    get: Callable            # (doc, row) -> value to watch
    holds: Callable          # (value, old) -> bool   precondition: documents currently show the old value
    expect: Callable         # (value_before, old, new) -> value after the change


def plain(index, query, field):
    return Check(index, query, lambda d, r: d.get(field, MISSING), lambda v, o: same(v, o), lambda v, o, n: n)


def element(index, query, array, id_field, id_of_row, field):
    def get(d, r):
        want = id_of_row(r)
        return next((x.get(field) for x in (d.get(array) or []) if x.get(id_field) == want), MISSING)
    return Check(index, query, get, lambda v, o: same(v, o), lambda v, o, n: n)


def every(index, query, array, field):
    return Check(index, query, lambda d, r: tuple(x.get(field) for x in (d.get(array) or [])),
                 lambda v, o: o in v, lambda v, o, n: tuple(n if e == o else e for e in v))


def mark(old):
    s = str(old)
    return s[:-1] + ("#" if s[-1] != "#" else "$")


def record(name, status, detail="", docs=0, t_apply=None, t_revert=None):
    RESULTS.append((name, status, detail, docs, t_apply, t_revert))
    flag = {"PASS": "PASS", "FAIL": "FAIL", "SKIP": "skip"}[status]
    extra = f" docs={docs} apply={t_apply:.0f}s revert={t_revert:.0f}s" if t_apply is not None and t_revert is not None else ""
    print(f"[{flag}] {name}{extra} {detail}", flush=True)


def selected(name):
    return not ONLY or any(o in name.lower() for o in ONLY)


# ----------------------------------------------------------------------------------------------- update scenarios
def update_scenario(name, table, col, pick_sql, make_checks, max_docs=20000, new_of=mark, wait_base=150):
    if not selected(name):
        return
    print(f"\n== {name}", flush=True)
    try:
        candidates = q(pick_sql)
    except Exception as exc:
        record(name, "FAIL", f"pick query failed: {exc}")
        return
    for row in candidates:
        rowid, old = row[0], row[1]
        if old is None or str(old) == "":
            continue
        checks = make_checks(row)
        snaps, ok, total = [], True, 0
        for ch in checks:
            try:
                docs = es_docs(ch.index, ch.query, max_docs)
            except TooBig:
                ok = False
                break
            if not docs or not all(ch.holds(ch.get(d, row), old) for d in docs.values()):
                ok = False
                break
            snaps.append(docs)
            total += len(docs)
        if not ok:
            continue
        new = new_of(old)

        def restore(rowid=rowid, old=old):
            dml(f"UPDATE emob.{table} SET {col} = :v WHERE rowid = :r", v=old, r=rowid)
        REVERTS.append((f"{table}.{col} {rowid} back to its original value", restore))
        try:
            dml(f"UPDATE emob.{table} SET {col} = :v WHERE rowid = :r", v=new, r=rowid)
        except oracledb.DatabaseError as exc:
            REVERTS.pop()
            if "ORA-02290" in str(exc) or "ORA-12899" in str(exc) or "ORA-02291" in str(exc):
                record(name, "SKIP", f"database refused the test value ({str(exc).splitlines()[0]})")
                return
            record(name, "FAIL", str(exc))
            return

        def applied():
            for ch, docs in zip(checks, snaps):
                now = mget(ch.index, docs)
                for i, before in docs.items():
                    if now.get(i) is None or not same(ch.get(now[i], row), ch.expect(ch.get(before, row), old, new)):
                        return False
            return True
        t1 = wait_until(applied, wait_base + total / 40)
        restore()
        REVERTS.pop()

        def reverted():
            for ch, docs in zip(checks, snaps):
                now = mget(ch.index, docs)
                for i, before in docs.items():
                    if now.get(i) is None or strip(now[i]) != strip(before):
                        return False
            return True
        t2 = wait_until(reverted, wait_base + total / 40)
        if t1 is None:
            record(name, "FAIL", f"documents did not take the new value ({total} docs watched)", total)
        elif t2 is None:
            diffs = []
            for ch, docs in zip(checks, snaps):
                now = mget(ch.index, docs)
                for i, before in docs.items():
                    a, b = strip(now.get(i)) or {}, strip(before)
                    for k in sorted(set(a) | set(b)):
                        if not same(a.get(k, MISSING), b.get(k, MISSING)):
                            diffs.append(f"{ch.index}/{i}.{k}: now={str(a.get(k, MISSING))[:90]} before={str(b.get(k, MISSING))[:90]}")
            record(name, "FAIL", "documents did not return to their original content after the revert: " + " | ".join(diffs[:4]),
                   total, t1, wait_base)
        else:
            record(name, "PASS", "", total, t1, t2)
        return
    record(name, "SKIP", "no suitable row (documents missing or fan-out above the cap)")


def term(index, field, value):
    return {"term": {keyword(index, field): value}}


# ----------------------------------------------------------------------------------------------- insert / delete scenarios
def clone(table, rowid, overrides):
    with conn.cursor() as cur:
        cur.execute(f"SELECT * FROM emob.{table} WHERE rownum = 0")
        cols = [d[0] for d in cur.description]
    sel = ", ".join(f":ov_{c.lower()}" if c.lower() in overrides else c for c in cols)
    binds = {f"ov_{k}": v for k, v in overrides.items()}
    binds["rid"] = rowid
    return dml(f"INSERT INTO emob.{table} ({', '.join(cols)}) SELECT {sel} FROM emob.{table} WHERE rowid = :rid", **binds)


def ac_no_of(ac_id):
    r = q("SELECT ac_no FROM emob.mb_account_mst WHERE ac_id = :a", a=ac_id)
    return r[0][0] if r else None


def accounts_of_customer(cust_no):
    return [r[0] for r in q("SELECT ac_no FROM emob.mb_account_mst WHERE cust_no = :c", c=cust_no)]


def child_clone_scenario(name, table, pick_sql, id_col, array, id_field, parent_accounts, fields, delete_where):
    """Insert a copy of a child row (nominee/document/address/operator): parent documents gain an element, then lose it."""
    if not selected(name):
        return
    print(f"\n== {name}", flush=True)
    for row in q(pick_sql):
        rowid = row[0]
        accts = parent_accounts(row)
        if not accts:
            continue
        before = mget(CUSTOMER, accts)
        if any(d is None or not d.get(array) for d in before.values()):
            continue
        new_id = random.randint(900_000_000, 999_999_999)
        overrides = {id_col: new_id} if id_col else {"create_by": "E2E"}
        where, binds = delete_where(new_id, row)

        def undo(where=where, binds=binds):
            dml(f"DELETE FROM emob.{table} WHERE {where}", **binds)
        REVERTS.append((f"delete the cloned {table} row", undo))
        try:
            clone(table, rowid, overrides)
        except oracledb.DatabaseError as exc:
            REVERTS.pop()
            record(name, "SKIP", f"database refused the clone ({str(exc).splitlines()[0]})")
            return

        def gained():
            now = mget(CUSTOMER, accts)
            return all(now[a] and len(now[a][array]) == len(before[a][array]) + 1 for a in accts)
        t1 = wait_until(gained, 150)
        detail = ""
        if t1 is not None:
            for a in accts:
                new_el = [x for x in mget(CUSTOMER, [a])[a][array] if x not in before[a][array]]
                src = [x for x in before[a][array] if all(x.get(f) == new_el[0].get(f) for f in fields)]
                if len(new_el) != 1 or not src:
                    detail = f"new element does not match its source row: {new_el}"
        undo()
        REVERTS.pop()

        def back():
            now = mget(CUSTOMER, accts)
            return all(now[a] and strip(now[a]) == strip(before[a]) for a in accts)
        t2 = wait_until(back, 150)
        if t1 is None:
            record(name, "FAIL", "parent document never gained the new element")
        elif detail:
            record(name, "FAIL", detail)
        elif t2 is None:
            record(name, "FAIL", "parent document did not return to its original content after the delete")
        else:
            record(name, "PASS", "", len(accts), t1, t2)
        return
    record(name, "SKIP", "no suitable row")


def main():
    # ---- plain field updates, one table at a time ------------------------------------------------
    update_scenario("transaction: update narration (direct key)", "mb_transaction_dtl", "naration",
                    "SELECT rowid, naration, tran_id FROM emob.mb_transaction_dtl SAMPLE (0.02) "
                    "WHERE naration IS NOT NULL AND LENGTH(naration) > 2 FETCH FIRST 40 ROWS ONLY",
                    lambda r: [plain(TRAN, term(TRAN, "tran_id", r[2]), "naration")])

    update_scenario("account: update title (customer doc + all balance docs)", "mb_account_mst", "ac_title",
                    "SELECT rowid, ac_title, ac_id FROM emob.mb_account_mst SAMPLE (0.5) WHERE ac_title IS NOT NULL "
                    "FETCH FIRST 60 ROWS ONLY",
                    lambda r: [plain(CUSTOMER, term(CUSTOMER, "ac_id", r[2]), "ac_title"),
                               plain(BALANCE, term(BALANCE, "ac_id", r[2]), "ac_title"),
                               plain(TRAN, term(TRAN, "ac_id", r[2]), "ac_title")])

    update_scenario("customer: update first name", "mb_customer_mst", "first_name",
                    "SELECT rowid, first_name, cust_no FROM emob.mb_customer_mst SAMPLE (0.5) WHERE first_name IS NOT NULL "
                    "FETCH FIRST 60 ROWS ONLY",
                    lambda r: [plain(CUSTOMER, term(CUSTOMER, "cust_no", r[2]), "first_name")])

    update_scenario("nominee: update name (child array element)", "nominee_mst", "nominee_name",
                    "SELECT rowid, nominee_name, nominee_id, ac_id FROM emob.nominee_mst SAMPLE (2) "
                    "WHERE nominee_name IS NOT NULL FETCH FIRST 60 ROWS ONLY",
                    lambda r: [element(CUSTOMER, term(CUSTOMER, "ac_id", r[3]), "nominee", "nominee_id",
                                       lambda row: row[2], "nominee_name")])

    update_scenario("address: update village (child array element)", "mb_address_mst", "village",
                    "SELECT rowid, village, address_id, ref_no FROM emob.mb_address_mst SAMPLE (2) "
                    "WHERE village IS NOT NULL FETCH FIRST 60 ROWS ONLY",
                    lambda r: [element(CUSTOMER, term(CUSTOMER, "cust_no", r[3]), "address", "address_id",
                                       lambda row: row[2], "village")])

    update_scenario("document: update number (child array element)", "mb_document_mst", "docment_no",
                    "SELECT rowid, docment_no, document_id, cust_no FROM emob.mb_document_mst SAMPLE (2) "
                    "WHERE docment_no IS NOT NULL FETCH FIRST 60 ROWS ONLY",
                    lambda r: [element(CUSTOMER, term(CUSTOMER, "cust_no", r[3]), "doc", "document_id",
                                       lambda row: row[2], "document_no")])

    update_scenario("operator: update status (child array element)", "mb_account_operator", "status",
                    "SELECT rowid, status, ac_id, cust_no FROM emob.mb_account_operator SAMPLE (5) x "
                    "WHERE status IS NOT NULL AND (SELECT COUNT(*) FROM emob.mb_account_operator o "
                    "WHERE o.ac_id = x.ac_id AND o.cust_no = x.cust_no) = 1 FETCH FIRST 60 ROWS ONLY",
                    lambda r: [element(CUSTOMER, term(CUSTOMER, "ac_id", r[2]), "operator", "cust_no",
                                       lambda row: row[3], "status")])

    update_scenario("balance: update amount (composite key with date)", "mb_account_daily_balance", "balance",
                    "SELECT rowid, balance, ac_id, value_date FROM emob.mb_account_daily_balance SAMPLE (0.05) "
                    "WHERE balance IS NOT NULL FETCH FIRST 40 ROWS ONLY",
                    lambda r: [plain(BALANCE, {"ids": {"values": [f"{r[2]}_{iso(r[3], TZ)}"]}}, "balance")],
                    new_of=lambda old: float(old) + 1234.5)

    # ---- dimension (lookup) tables: one change fans out to many documents ----------------------------
    update_scenario("product: update description (fan-out, all three indices)", "mb_product_mst", "full_desc",
                    "SELECT rowid, full_desc, ac_type_id FROM emob.mb_product_mst p WHERE full_desc IS NOT NULL AND "
                    "(SELECT COUNT(*) FROM emob.mb_product_mst p2 WHERE p2.full_desc = p.full_desc) = 1 ORDER BY dbms_random.value",
                    lambda r: [plain(CUSTOMER, term(CUSTOMER, "ac_type_id", r[2]), "full_desc"),
                               plain(BALANCE, term(BALANCE, "product", r[1]), "product"),
                               plain(TRAN, term(TRAN, "ac_type_id", r[2]), "product_name")], max_docs=40000)

    update_scenario("agent point: update name (fan-out, both indices)", "st_agent_point", "point_name",
                    "SELECT rowid, point_name, point_id FROM emob.st_agent_point p WHERE point_name IS NOT NULL AND "
                    "(SELECT COUNT(*) FROM emob.st_agent_point p2 WHERE p2.point_name = p.point_name) = 1 "
                    "AND point_id IN (SELECT point_id FROM emob.mb_account_mst) ORDER BY dbms_random.value",
                    lambda r: [plain(CUSTOMER, {"bool": {"must": [term(CUSTOMER, "ac_point_id", r[2]),
                                                                  term(CUSTOMER, "agent_point_id", r[2])]}}, "point_name"),
                               plain(BALANCE, term(BALANCE, "point_name", r[1]), "point_name")], max_docs=15000)

    update_scenario("point category: update name", "st_point_category", "category_name",
                    "SELECT rowid, category_name, point_category_id FROM emob.st_point_category c WHERE category_name IS NOT NULL "
                    "AND (SELECT COUNT(*) FROM emob.st_point_category c2 WHERE c2.category_name = c.category_name) = 1 "
                    "ORDER BY dbms_random.value",
                    lambda r: [plain(CUSTOMER, term(CUSTOMER, "point_category", r[1]), "point_category"),
                               plain(BALANCE, term(BALANCE, "point_category", r[1]), "point_category")], max_docs=30000)

    update_scenario("district: update name (address array + balance)", "district_info", "district_name",
                    "SELECT rowid, district_name, district_code FROM emob.district_info d WHERE district_name IS NOT NULL AND "
                    "(SELECT COUNT(*) FROM emob.district_info d2 WHERE d2.district_name = d.district_name) = 1 "
                    "ORDER BY dbms_random.value",
                    lambda r: [every(CUSTOMER, {"term": {"address.district": r[1]}}, "address", "district"),
                               plain(BALANCE, term(BALANCE, "district", r[1]), "district")], max_docs=20000)

    update_scenario("upazila: update name (address array)", "upazila_info", "upazila_name",
                    "SELECT rowid, upazila_name, upazila_code FROM emob.upazila_info u WHERE upazila_name IS NOT NULL AND "
                    "(SELECT COUNT(*) FROM emob.upazila_info u2 WHERE u2.upazila_name = u.upazila_name) = 1 "
                    "ORDER BY dbms_random.value",
                    lambda r: [every(CUSTOMER, {"term": {"address.upazilla": r[1]}}, "address", "upazilla")], max_docs=10000)

    update_scenario("union: update name (address array)", "union_info", "union_name",
                    "SELECT rowid, union_name, union_code FROM emob.union_info u WHERE union_name IS NOT NULL AND "
                    "(SELECT COUNT(*) FROM emob.union_info u2 WHERE u2.union_name = u.union_name) = 1 "
                    "ORDER BY dbms_random.value",
                    lambda r: [every(CUSTOMER, {"term": {"address.union": r[1]}}, "address", "union")], max_docs=5000)

    update_scenario("code table: update occupation text", "stcodmas", "coddes",
                    "SELECT rowid, coddes, sofcod FROM emob.stcodmas s WHERE harcod = 'OCC' AND coddes IS NOT NULL AND "
                    "(SELECT COUNT(*) FROM emob.stcodmas s2 WHERE s2.harcod = 'OCC' AND s2.coddes = s.coddes) = 1 "
                    "AND (SELECT COUNT(*) FROM emob.stcodmas s3 WHERE s3.harcod = 'EDT' AND s3.coddes = s.coddes) = 0 "
                    "ORDER BY dbms_random.value",
                    lambda r: [plain(CUSTOMER, term(CUSTOMER, "occupation", r[1]), "occupation"),
                               plain(BALANCE, term(BALANCE, "occupation", r[1]), "occupation"),
                               plain(TRAN, term(TRAN, "occupation", r[1]), "occupation")], max_docs=40000)

    update_scenario("code table: update education text", "stcodmas", "coddes",
                    "SELECT rowid, coddes, sofcod FROM emob.stcodmas s WHERE harcod = 'EDT' AND coddes IS NOT NULL AND "
                    "(SELECT COUNT(*) FROM emob.stcodmas s2 WHERE s2.harcod = 'EDT' AND s2.coddes = s.coddes) = 1 "
                    "AND (SELECT COUNT(*) FROM emob.stcodmas s3 WHERE s3.harcod = 'OCC' AND s3.coddes = s.coddes) = 0 "
                    "ORDER BY dbms_random.value",
                    lambda r: [plain(CUSTOMER, term(CUSTOMER, "education", r[1]), "education"),
                               plain(BALANCE, term(BALANCE, "education", r[1]), "education")], max_docs=20000)

    update_scenario("operating instruction: update text", "st_operating_instruction", "ins_desc",
                    "SELECT rowid, ins_desc, opr_ins FROM emob.st_operating_instruction i WHERE ins_desc IS NOT NULL AND "
                    "(SELECT COUNT(*) FROM emob.st_operating_instruction i2 WHERE i2.ins_desc = i.ins_desc) = 1 "
                    "ORDER BY dbms_random.value",
                    lambda r: [plain(CUSTOMER, term(CUSTOMER, "operating_ins", r[1]), "operating_ins")], max_docs=20000)

    # ---- inserts and deletes ----------------------------------------------------------------------
    insert_delete_tests()


def insert_ignored_test():
    """config.yaml applies updates and deletes only: an inserted row must not create a document."""
    name = "transaction: an inserted row is ignored (no document), its delete is harmless"
    if not selected(name):
        return
    print(f"\n== {name}", flush=True)
    for rowid, tran_id in q("SELECT rowid, tran_id FROM emob.mb_transaction_dtl SAMPLE (0.02) FETCH FIRST 30 ROWS ONLY"):
        if mget(TRAN, [str(tran_id)])[str(tran_id)] is None:
            continue
        new_id = random.randint(900_000_000, 999_999_999)
        count_before = client.count(index=TRAN)["count"]
        undo = lambda n=new_id: dml("DELETE FROM emob.mb_transaction_dtl WHERE tran_id = :t", t=n)
        REVERTS.append(("delete the cloned transaction row", undo))
        try:
            clone("mb_transaction_dtl", rowid, {"tran_id": new_id})
        except oracledb.DatabaseError as exc:
            REVERTS.pop()
            record(name, "SKIP", f"database refused the clone ({str(exc).splitlines()[0]})")
            return
        appeared = wait_until(lambda: mget(TRAN, [str(new_id)])[str(new_id)] is not None, 40)
        count_mid = client.count(index=TRAN)["count"]
        undo()
        REVERTS.pop()
        time.sleep(30)               # long enough for Debezium to publish the delete and the service to read it
        gone = mget(TRAN, [str(new_id)])[str(new_id)] is None
        count_after = client.count(index=TRAN)["count"]
        if appeared is not None:
            record(name, "FAIL", "a document was created for the inserted row, but inserts are switched off")
        elif not gone or count_mid != count_before or count_after != count_before:
            record(name, "FAIL", f"counts {count_before} -> {count_mid} -> {count_after}")
        else:
            record(name, "PASS", f"no document appeared; counts stayed {count_before}", 1, 40, 30)
        return
    record(name, "SKIP", "no suitable row")


def nested_scenario(name, table, pick_sql, id_col, array, parents_of, text_col, es_key, clone_where):
    """Life of a nested array element when inserts are switched off. Only a CLONE of a child row is ever touched:
         1. insert the clone                -> ignored: the parent documents do not change at all
         2. update a text column of it      -> the array gains the element (the update refreshes it from Oracle)
         3. update it again                 -> the element changes, the array keeps its size
         4. delete the clone                -> the element disappears; the documents equal their original content
       Everything else in the parent documents must stay byte-identical throughout."""
    if not selected(name):
        return
    print(f"\n== {name}", flush=True)
    M1, M2 = "E2E1", "E2E2"
    for row in q(pick_sql):
        rowid, accts = row[0], parents_of(row)
        if not accts:
            continue
        before = mget(CUSTOMER, accts)
        if any(d is None or not d.get(array) for d in before.values()):
            continue
        new_id = random.randint(900_000_000, 999_999_999)
        overrides = {id_col: new_id} if id_col else {"create_by": "E2E"}
        where, binds = clone_where(new_id, row)

        def undo(where=where, binds=binds):
            dml(f"DELETE FROM emob.{table} WHERE {where}", **binds)

        def set_text(value, where=where, binds=binds):
            dml(f"UPDATE emob.{table} SET {text_col} = :v WHERE {where}", v=value, **binds)

        def others(doc):                        # everything except the array
            return {k: v for k, v in strip(doc).items() if k != array}

        def element_of(doc, marker):
            return [x for x in doc[array] if x.get(es_key) == marker]

        def without(doc, marker):
            return [x for x in doc[array] if x.get(es_key) != marker]

        REVERTS.append((f"delete the cloned {table} row", undo))
        try:
            clone(table, rowid, overrides)
        except oracledb.DatabaseError as exc:
            REVERTS.pop()
            record(name, "SKIP", f"database refused the clone ({str(exc).splitlines()[0]})")
            return
        problems, t_total = [], time.time()

        time.sleep(25)                                               # 1. the insert must change nothing
        now = mget(CUSTOMER, accts)
        if any(now[a] is None or strip(now[a]) != strip(before[a]) for a in accts):
            problems.append("the inserted row changed a parent document, but inserts are switched off")

        try:
            set_text(M1)                                             # 2. update brings the element in
            t2 = wait_until(lambda: all(len(mget(CUSTOMER, [a])[a][array]) == len(before[a][array]) + 1 for a in accts), 90)
            now = mget(CUSTOMER, accts)
            if t2 is None:
                problems.append("the update of the cloned row did not add the element to the array")
            else:
                for a in accts:
                    if len(element_of(now[a], M1)) != 1:
                        problems.append(f"{a}: expected exactly one element with {es_key}={M1}")
                    if without(now[a], M1) != before[a][array]:
                        problems.append(f"{a}: the other elements of {array} changed")
                    if others(now[a]) != others(before[a]):
                        problems.append(f"{a}: fields outside {array} changed")

            set_text(M2)                                             # 3. second update changes the element
            t3 = wait_until(lambda: all(len(element_of(mget(CUSTOMER, [a])[a], M2)) == 1 for a in accts), 90)
            now = mget(CUSTOMER, accts)
            if t3 is None:
                problems.append("the second update did not change the element")
            elif any(len(now[a][array]) != len(before[a][array]) + 1 or element_of(now[a], M1) for a in accts):
                problems.append("after the second update the array has the wrong size or still shows the old value")
        except oracledb.DatabaseError as exc:
            problems.append(f"database refused the test value: {str(exc).splitlines()[0]}")

        undo()                                                       # 4. delete removes it again
        REVERTS.pop()
        t4 = wait_until(lambda: all(strip(mget(CUSTOMER, [a])[a]) == strip(before[a]) for a in accts), 90)
        if t4 is None:
            problems.append("after the delete the documents did not return to their original content")
        record(name, "FAIL" if problems else "PASS", "; ".join(problems) or
               f"{len(accts)} parent document(s): insert ignored, update added the element, 2nd update changed it, delete removed it",
               len(accts), time.time() - t_total, 0)
        return
    record(name, "SKIP", "no suitable row")


def insert_delete_tests():
    if "c" not in OPERATIONS:
        # The scenarios below insert a row and expect a document: they cannot pass with inserts switched off.
        insert_ignored_test()
        nested_scenario("nominee: nested element lifecycle with inserts off", "nominee_mst",
                        "SELECT rowid, ac_id, nominee_id FROM emob.nominee_mst SAMPLE (2) FETCH FIRST 40 ROWS ONLY",
                        "nominee_id", "nominee", lambda r: [a for a in [ac_no_of(r[1])] if a], "nominee_name", "nominee_name",
                        lambda n, r: ("nominee_id = :n", {"n": n}))
        nested_scenario("document: nested element lifecycle with inserts off", "mb_document_mst",
                        "SELECT rowid, cust_no, document_id FROM emob.mb_document_mst SAMPLE (2) FETCH FIRST 40 ROWS ONLY",
                        "document_id", "doc", lambda r: accounts_of_customer(r[1]), "issue_place", "issue_place",
                        lambda n, r: ("document_id = :n", {"n": n}))
        nested_scenario("address: nested element lifecycle with inserts off", "mb_address_mst",
                        "SELECT rowid, ref_no, address_id FROM emob.mb_address_mst SAMPLE (2) FETCH FIRST 40 ROWS ONLY",
                        "address_id", "address", lambda r: accounts_of_customer(r[1]), "village", "village",
                        lambda n, r: ("address_id = :n", {"n": n}))
        nested_scenario("operator: nested element lifecycle with inserts off", "mb_account_operator",
                        "SELECT rowid, ac_id, cust_no FROM emob.mb_account_operator SAMPLE (5) FETCH FIRST 40 ROWS ONLY",
                        None, "operator", lambda r: [a for a in [ac_no_of(r[1])] if a], "inactive_by", "inactive_by",
                        lambda n, r: ("create_by = 'E2E' AND ac_id = :a", {"a": r[1]}))
        for skipped in ("transaction: insert a row then delete it (document created, then removed)",
                        "balance: insert a row for a new date then delete it (composite key)",
                        "account: insert, change its account number (key change), delete"):
            if selected(skipped):
                record(skipped, "SKIP", "inserts are switched off in config.yaml (sync.operations)")
        return
    name = "transaction: insert a row then delete it (document created, then removed)"
    if selected(name):
        print(f"\n== {name}", flush=True)
        src = q("SELECT rowid, tran_id, ac_id, naration FROM emob.mb_transaction_dtl SAMPLE (0.02) FETCH FIRST 30 ROWS ONLY")
        for rowid, tran_id, ac_id, nar in src:
            if mget(TRAN, [str(tran_id)])[str(tran_id)] is None:
                continue
            new_id = random.randint(900_000_000, 999_999_999)
            count_before = client.count(index=TRAN)["count"]
            undo = lambda n=new_id: dml("DELETE FROM emob.mb_transaction_dtl WHERE tran_id = :t", t=n)
            REVERTS.append(("delete the cloned transaction row", undo))
            try:
                clone("mb_transaction_dtl", rowid, {"tran_id": new_id})
            except oracledb.DatabaseError as exc:
                REVERTS.pop()
                record(name, "SKIP", f"database refused the clone ({str(exc).splitlines()[0]})")
                break
            t1 = wait_until(lambda: mget(TRAN, [str(new_id)])[str(new_id)] is not None, 150)
            doc = mget(TRAN, [str(new_id)])[str(new_id)]
            ok_doc = bool(doc) and doc["tran_id"] == new_id and doc["ac_id"] == ac_id
            undo()
            REVERTS.pop()
            t2 = wait_until(lambda: mget(TRAN, [str(new_id)])[str(new_id)] is None, 150)
            count_after = client.count(index=TRAN)["count"]
            if t1 is None or not ok_doc:
                record(name, "FAIL", "document was not created correctly")
            elif t2 is None or count_after != count_before:
                record(name, "FAIL", f"document not removed (count {count_before} -> {count_after})")
            else:
                record(name, "PASS", "", 1, t1, t2)
            break
        else:
            record(name, "SKIP", "no suitable row")

    name = "balance: insert a row for a new date then delete it (composite key)"
    if selected(name):
        print(f"\n== {name}", flush=True)
        src = q("SELECT rowid, ac_id, balance FROM emob.mb_account_daily_balance SAMPLE (0.05) FETCH FIRST 30 ROWS ONLY")
        for rowid, ac_id, bal in src:
            new_date = datetime(1999, 1, 1)
            doc_id = f"{ac_id}_{iso(new_date, TZ)}"
            if mget(BALANCE, [doc_id])[doc_id] is not None:
                continue
            count_before = client.count(index=BALANCE)["count"]
            undo = lambda a=ac_id: dml("DELETE FROM emob.mb_account_daily_balance WHERE ac_id = :a AND value_date = :d", a=a, d=new_date)
            REVERTS.append(("delete the cloned balance row", undo))
            try:
                clone("mb_account_daily_balance", rowid, {"value_date": new_date})
            except oracledb.DatabaseError as exc:
                REVERTS.pop()
                record(name, "SKIP", f"database refused the clone ({str(exc).splitlines()[0]})")
                break
            t1 = wait_until(lambda: mget(BALANCE, [doc_id])[doc_id] is not None, 150)
            doc = mget(BALANCE, [doc_id])[doc_id]
            ok_doc = bool(doc) and same(doc["balance"], bal) and doc["balance_date"] == iso(new_date, TZ)
            undo()
            REVERTS.pop()
            t2 = wait_until(lambda: mget(BALANCE, [doc_id])[doc_id] is None, 150)
            count_after = client.count(index=BALANCE)["count"]
            if t1 is None or not ok_doc:
                record(name, "FAIL", f"document {doc_id} was not created correctly: {doc and doc.get('balance_date')}")
            elif t2 is None or count_after != count_before:
                record(name, "FAIL", f"document not removed (count {count_before} -> {count_after})")
            else:
                record(name, "PASS", f"id {doc_id}", 1, t1, t2)
            break
        else:
            record(name, "SKIP", "no suitable row")

    child_clone_scenario("nominee: insert then delete (parent document gains/loses an element)", "nominee_mst",
                         "SELECT rowid, ac_id, nominee_id FROM emob.nominee_mst SAMPLE (2) FETCH FIRST 40 ROWS ONLY",
                         "nominee_id", "nominee", "nominee_id", lambda r: [a for a in [ac_no_of(r[1])] if a],
                         ["nominee_name", "relation"], lambda n, r: ("nominee_id = :n", {"n": n}))
    child_clone_scenario("document: insert then delete", "mb_document_mst",
                         "SELECT rowid, cust_no, document_id FROM emob.mb_document_mst SAMPLE (2) FETCH FIRST 40 ROWS ONLY",
                         "document_id", "doc", "document_id", lambda r: accounts_of_customer(r[1]),
                         ["document_type", "document_no"], lambda n, r: ("document_id = :n", {"n": n}))
    child_clone_scenario("address: insert then delete", "mb_address_mst",
                         "SELECT rowid, ref_no, address_id FROM emob.mb_address_mst SAMPLE (2) FETCH FIRST 40 ROWS ONLY",
                         "address_id", "address", "address_id", lambda r: accounts_of_customer(r[1]),
                         ["address_type", "village"], lambda n, r: ("address_id = :n", {"n": n}))
    child_clone_scenario("operator: insert then delete (identified by a marker)", "mb_account_operator",
                         "SELECT rowid, ac_id, cust_no FROM emob.mb_account_operator SAMPLE (5) FETCH FIRST 40 ROWS ONLY",
                         None, "operator", "cust_no", lambda r: [a for a in [ac_no_of(r[1])] if a],
                         ["cust_no", "status"], lambda n, r: ("create_by = 'E2E' AND ac_id = :a", {"a": r[1]}))

    name = "account: insert, change its account number (key change), delete"
    if selected(name):
        print(f"\n== {name}", flush=True)
        src = q("SELECT rowid, ac_id, ac_no, cust_no FROM emob.mb_account_mst SAMPLE (0.5) FETCH FIRST 30 ROWS ONLY")
        for rowid, ac_id, ac_no, cust_no in src:
            if mget(CUSTOMER, [ac_no])[ac_no] is None:
                continue
            n = random.randint(100000, 999999)
            new_id, no1, no2 = random.randint(800_000_000, 899_999_999), f"E2E{n}", f"E2F{n}"
            count_before = client.count(index=CUSTOMER)["count"]
            undo = lambda a=new_id: dml("DELETE FROM emob.mb_account_mst WHERE ac_id = :a", a=a)
            REVERTS.append(("delete the cloned account", undo))
            try:
                clone("mb_account_mst", rowid, {"ac_id": new_id, "ac_no": no1})
            except oracledb.DatabaseError as exc:
                REVERTS.pop()
                record(name, "SKIP", f"database refused the clone ({str(exc).splitlines()[0]})")
                break
            t1 = wait_until(lambda: mget(CUSTOMER, [no1])[no1] is not None, 150)
            doc1 = mget(CUSTOMER, [no1])[no1]
            ok1 = bool(doc1) and doc1["cust_no"] == cust_no and doc1["ac_id"] == new_id
            dml("UPDATE emob.mb_account_mst SET ac_no = :n WHERE ac_id = :a", n=no2, a=new_id)
            t2 = wait_until(lambda: mget(CUSTOMER, [no2])[no2] is not None and mget(CUSTOMER, [no1])[no1] is None, 150)
            undo()
            REVERTS.pop()
            t3 = wait_until(lambda: mget(CUSTOMER, [no1, no2]) == {no1: None, no2: None}, 150)
            count_after = client.count(index=CUSTOMER)["count"]
            if t1 is None or not ok1:
                record(name, "FAIL", "document for the new account was not created correctly")
            elif t2 is None:
                record(name, "FAIL", "after the key change the old document was not removed or the new one not created")
            elif t3 is None or count_after != count_before:
                record(name, "FAIL", f"documents not removed after delete (count {count_before} -> {count_after})")
            else:
                record(name, "PASS", "insert, key change and delete all applied", 1, t1, t3)
            break
        else:
            record(name, "SKIP", "no suitable row")


if __name__ == "__main__":
    counts_before = {i: client.count(index=i)["count"] for i in (CUSTOMER, BALANCE, TRAN)}
    try:
        main()
    except Exception:
        traceback.print_exc()
        RESULTS.append(("harness", "FAIL", "exception, see above", 0, None, None))
    finally:
        run_reverts()
    time.sleep(3)
    counts_after = {i: client.count(index=i)["count"] for i in counts_before}
    print("\n==================== SUMMARY")
    for name, status, detail, docs, ta, tr in RESULTS:
        print(f"{status:5} {name}  {detail}")
    print("document counts before:", counts_before)
    print("document counts after: ", counts_after, "(identical)" if counts_before == counts_after else "<-- DIFFERENT")
    failed = [r for r in RESULTS if r[1] == "FAIL"]
    sys.exit(1 if failed or counts_before != counts_after else 0)
