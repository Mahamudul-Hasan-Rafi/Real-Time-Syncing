"""Live fault-injection tests. Run AFTER e2e.py, with nothing else using the service.

Stops the normally running service, starts its own instances (optionally behind TCP proxies that this script
kills to simulate outages), makes real reversible changes in Oracle, and checks nothing is lost or duplicated.

    python tests/live/fault.py [fragment ...]       fragments: es-outage oracle-outage crash poison replay
"""
import json
import os
import re
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import e2e                      # reuses connections and helpers; its main() only runs under __main__
from e2e import CUSTOMER, TRAN, client, dml, mget, q, wait_until, strip, REVERTS, run_reverts

from confluent_kafka import Consumer, Producer, TopicPartition

ROOT = Path(__file__).resolve().parents[2]
PY = str(ROOT / ".venv" / "Scripts" / "python.exe")
LOGS = ROOT / "logs"
LOGS.mkdir(exist_ok=True)       # created on demand: the folder is not kept in the project
ONLY = [a.lower() for a in sys.argv[1:]]
KAFKA = e2e.cfg["kafka"]["bootstrap_servers"]
RESULTS = []
PROCS = []


def selected(name):
    return not ONLY or any(o in name for o in ONLY)


def result(name, ok, detail=""):
    RESULTS.append((name, ok, detail))
    print(f"[{'PASS' if ok else 'FAIL'}] {name} {detail}", flush=True)


# ------------------------------------------------------------------------------------------ processes
def taskkill(pid):
    subprocess.run(["taskkill", "/F", "/T", "/PID", str(pid)], capture_output=True)


def stop_main_service():
    pid_file = LOGS / "service.pid"
    if pid_file.exists():
        taskkill(int(pid_file.read_text().strip()))
        pid_file.unlink()
        time.sleep(3)


def start_main_service():
    out = open(LOGS / "service.out", "ab")
    err = open(LOGS / "service.err", "ab")
    p = subprocess.Popen([PY, "-m", "cdc_es_sync", "run"], cwd=ROOT, stdout=out, stderr=err)
    (LOGS / "service.pid").write_text(str(p.pid))


def start_service(tag, env=None):
    e = dict(os.environ)
    e.update(env or {})
    e["CDC_HTTP_PORT"] = "9201"
    err = open(LOGS / f"fault-{tag}.err", "wb")
    p = subprocess.Popen([PY, "-m", "cdc_es_sync", "run"], cwd=ROOT, stdout=subprocess.DEVNULL, stderr=err, env=e)
    PROCS.append(p)
    return p


def start_proxy(listen, host, port):
    p = subprocess.Popen([PY, str(Path(__file__).with_name("tcp_proxy.py")), str(listen), host, str(port)])
    PROCS.append(p)
    time.sleep(1.5)
    return p


def health(port=9201):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=3) as r:
            return json.loads(r.read())
    except Exception:
        return None


def wait_running(timeout=90):
    return wait_until(lambda: (health() or {}).get("state") == "running", timeout, 1)


def metric(name, port=9201):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=3) as r:
            text = r.read().decode()
        return sum(float(m.group(1)) for m in re.finditer(rf"^{name}(?:{{[^}}]*}})? ([0-9.e+-]+)$", text, re.M))
    except Exception:
        return None


def wait_assigned(tag, timeout=150):
    """After a hard kill the broker keeps the dead member until its session times out (45 s), so a new
    instance only receives partitions then. Faults must be injected after that, or they test nothing."""
    ok = wait_until(lambda: log_has(tag, r"assigned \[.emob\.EMOB"), timeout, 1)
    time.sleep(3)
    return ok is not None


def log_has(tag, pattern):
    path = LOGS / f"fault-{tag}.err"
    return bool(path.exists() and re.search(pattern, path.read_text(errors="replace")))


def cleanup_procs():
    for p in PROCS:
        if p.poll() is None:
            taskkill(p.pid)
    PROCS.clear()


# ------------------------------------------------------------------------------------------ test data
def pick_rows(n):
    """n real transactions that have a document, each with a narration we can change and restore."""
    rows = []
    for rowid, tran_id, nar in q("SELECT rowid, tran_id, naration FROM emob.mb_transaction_dtl SAMPLE (0.05) "
                                 "WHERE naration IS NOT NULL AND LENGTH(naration) > 2 FETCH FIRST 400 ROWS ONLY"):
        if len(rows) >= n:
            break
        rows.append((rowid, str(tran_id), nar))
    have = mget(TRAN, [r[1] for r in rows])
    return [r for r in rows if have[r[1]] is not None and have[r[1]].get("naration") == r[2]][:n]


def change_all(rows, suffix):
    for rowid, _, nar in rows:
        dml("UPDATE emob.mb_transaction_dtl SET naration = :v WHERE rowid = :r", v=e2e.mark(nar) if suffix else nar, r=rowid)


def register_restore(rows):
    def restore():
        for rowid, _, nar in rows:
            dml("UPDATE emob.mb_transaction_dtl SET naration = :v WHERE rowid = :r", v=nar, r=rowid)
    REVERTS.append(("restore the narration of the test transactions", restore))


def docs_show(rows, changed):
    now = mget(TRAN, [r[1] for r in rows])
    return all(now[r[1]] and now[r[1]].get("naration") == (e2e.mark(r[2]) if changed else r[2]) for r in rows)


def finish_cycle(name, rows):
    """Put the data back and make sure the documents follow."""
    change_all(rows, False)
    REVERTS.clear()
    back = wait_until(lambda: docs_show(rows, False), 120)
    if back is None:
        result(name + " (restore)", False, "documents did not return to their original narration")


# ------------------------------------------------------------------------------------------ tests
def t_es_outage():
    name = "es-outage: Elasticsearch unreachable while changes arrive"
    if not selected("es-outage"):
        return
    print(f"\n== {name}", flush=True)
    rows = pick_rows(25)
    proxy = start_proxy(19200, "10.11.200.109", 9200)
    start_service("es", {"ES_URL": "https://127.0.0.1:19200"})
    if wait_running() is None or not wait_assigned("es"):
        result(name, False, "service did not start or never received its partitions")
        return
    register_restore(rows)
    proxy.kill()
    time.sleep(1)
    change_all(rows, True)
    time.sleep(35)
    held = docs_show(rows, False) and (health() or {}).get("state") == "running"
    retrying = log_has("es", r"Elasticsearch unreachable")
    start_proxy(19200, "10.11.200.109", 9200)
    t = wait_until(lambda: docs_show(rows, True), 120)
    result(name, held and retrying and t is not None,
           f"held during outage={held}, retry logged={retrying}, caught up {t and round(t)}s after the link returned")
    finish_cycle(name, rows)
    cleanup_procs()


def t_oracle_outage():
    name = "oracle-outage: Oracle unreachable while changes arrive"
    if not selected("oracle-outage"):
        return
    print(f"\n== {name}", flush=True)
    rows = pick_rows(25)
    proxy = start_proxy(11521, "10.11.200.37", 1521)
    dsn = ("(DESCRIPTION=(ENABLE=BROKEN)(RETRY_COUNT=1)(RETRY_DELAY=2)(TRANSPORT_CONNECT_TIMEOUT=5)(CONNECT_TIMEOUT=10)"
           "(ADDRESS=(PROTOCOL=TCP)(HOST=127.0.0.1)(PORT=11521))(CONNECT_DATA=(SERVICE_NAME=emob)))")
    start_service("oracle", {"ORACLE_DSN": dsn})
    if wait_running() is None or not wait_assigned("oracle"):
        result(name, False, "service did not start or never received its partitions")
        return
    register_restore(rows)
    proxy.kill()
    time.sleep(1)
    change_all(rows, True)
    time.sleep(40)
    held = docs_show(rows, False) and (health() or {}).get("state") == "running"
    retrying = log_has("oracle", r"transient failure, retrying batch")
    start_proxy(11521, "10.11.200.37", 1521)
    t = wait_until(lambda: docs_show(rows, True), 150)
    result(name, held and retrying and t is not None,
           f"held during outage={held}, retry logged={retrying}, caught up {t and round(t)}s after the link returned")
    finish_cycle(name, rows)
    cleanup_procs()


def t_crash():
    name = "crash: service killed while changes are waiting and mid-way through a large rebuild"
    if not selected("crash"):
        return
    print(f"\n== {name}", flush=True)
    rows = pick_rows(25)
    start_service("crash1")
    if wait_running() is None or not wait_assigned("crash1"):
        result(name, False, "service did not start or never received its partitions")
        return
    register_restore(rows)
    # 1. changes committed, service killed at once, before it has applied them
    change_all(rows, True)
    cleanup_procs()
    pending = not docs_show(rows, True)
    start_service("crash2")
    t1 = wait_until(lambda: docs_show(rows, True), 120)
    ok1 = t1 is not None
    detail = f"{'some' if pending else 'no'} documents were unapplied at kill time; all applied {t1 and round(t1)}s after restart"
    # 2. kill in the middle of a large fan-out
    cands = q("SELECT rowid, full_desc, ac_type_id FROM emob.mb_product_mst p WHERE full_desc IS NOT NULL AND "
              "(SELECT COUNT(*) FROM emob.mb_product_mst p2 WHERE p2.full_desc = p.full_desc) = 1")
    best = None
    for rowid, desc, tid in cands:
        n = client.count(index=CUSTOMER, body={"query": {"term": {"ac_type_id": tid}}})["count"]
        if 3000 <= n <= 20000 and (best is None or n < best[3]):
            best = (rowid, desc, tid, n)
    ok2, detail2 = True, "no product with 3,000-20,000 accounts to use for the mid-rebuild kill"
    if best:
        rowid, desc, tid, n = best
        new = e2e.mark(desc)
        ids = [h["_id"] for h in e2e.helpers.scan(client, index=CUSTOMER, query={"query": {"term": {"ac_type_id": tid}}}, _source=False, size=2000)]

        def restore():
            dml("UPDATE emob.mb_product_mst SET full_desc = :v WHERE rowid = :r", v=desc, r=rowid)
        REVERTS.append(("restore the product description", restore))
        dml("UPDATE emob.mb_product_mst SET full_desc = :v WHERE rowid = :r", v=new, r=rowid)

        def done_count():
            return client.count(index=CUSTOMER, body={"query": {"term": {"full_desc.keyword": new}}})["count"]
        killed_at = None
        deadline = time.time() + 120
        while time.time() < deadline:
            c = done_count()
            if 0 < c < n:
                cleanup_procs()
                killed_at = c
                break
            time.sleep(0.4)
        start_service("crash3")
        full = wait_until(lambda: done_count() == n, 600, 3)
        restore()
        REVERTS.pop()
        back = wait_until(lambda: client.count(index=CUSTOMER, body={"query": {"term": {"full_desc.keyword": desc}}})["count"] == n
                          and done_count() == 0, 600, 3)
        ok2 = full is not None and back is not None and killed_at is not None
        detail2 = (f"killed with {killed_at} of {n} documents rebuilt; after restart all {n} done in {full and round(full)}s; "
                   f"restore {'ok' if back else 'FAILED'}" if killed_at else
                   f"rebuild finished before the kill could land ({n} docs); no mid-way kill")
    result(name, ok1 and ok2, f"{detail}; {detail2}")
    finish_cycle(name, rows)
    cleanup_procs()


def t_poison():
    name = "poison: malformed events, missing key columns, delete of a row that never existed, duplicates"
    if not selected("poison"):
        return
    print(f"\n== {name}", flush=True)
    topic = "emob.EMOB.MB_TRANSACTION_DTL"

    def dlq_end():
        c = Consumer({"bootstrap.servers": KAFKA, "group.id": "probe-" + str(time.time()), "enable.auto.commit": False})
        try:
            low, high = c.get_watermark_offsets(TopicPartition("cdc.es-sync.dlq", 0), timeout=10)
            return high
        finally:
            c.close()
    before = dlq_end()
    rows = pick_rows(3)
    start_service("poison")
    if wait_running() is None or not wait_assigned("poison"):
        result(name, False, "service did not start or never received its partitions")
        return
    register_restore(rows)
    prod = Producer({"bootstrap.servers": KAFKA})
    env = lambda op, b, a: json.dumps({"payload": {"op": op, "before": b, "after": a,
                                                     "source": {"table": "MB_TRANSACTION_DTL", "scn": "1", "ts_ms": 1}}})
    prod.produce(topic, b"this is not json")
    prod.produce(topic, env("u", {"NOT_A_KEY": "1"}, {"NOT_A_KEY": "1"}).encode())          # key column missing
    prod.produce(topic, env("x", None, {"TRAN_ID": "1"}).encode())                              # unsupported operation
    prod.produce(topic, env("d", {"TRAN_ID": "987654321"}, None).encode())                      # row never existed: harmless
    for _ in range(40):                                                                          # duplicates of a real key
        prod.produce(topic, env("u", {"TRAN_ID": rows[0][1]}, {"TRAN_ID": rows[0][1]}).encode())
    prod.flush(15)
    time.sleep(10)
    alive = (health() or {}).get("state") == "running"
    change_all(rows, True)                                                                      # the stream still works afterwards
    t = wait_until(lambda: docs_show(rows, True), 90)
    after = dlq_end()
    new_dead = after - before
    result(name, alive and t is not None and new_dead == 3,
           f"service alive={alive}, dead letters added={new_dead} (expected 3), later change applied in {t and round(t)}s")
    finish_cycle(name, rows)
    cleanup_procs()


def t_replay():
    name = "replay: a brand-new consumer group re-reads EVERY event ever published (idempotence)"
    if not selected("replay"):
        return
    print(f"\n== {name}", flush=True)
    counts_before = {i: client.count(index=i)["count"] for i in (CUSTOMER, e2e.BALANCE, TRAN)}
    group = f"replay-{int(time.time())}"
    p = start_service("replay", {"CDC_GROUP": group, "CDC_OFFSET_RESET": "earliest"})
    if wait_running() is None:
        result(name, False, "service did not start")
        return
    wait_assigned("replay")
    t0, drained, polls = time.time(), 0, 0
    while time.time() - t0 < 1800:
        lag_text = None
        try:
            with urllib.request.urlopen("http://127.0.0.1:9201/metrics", timeout=3) as r:
                lag_text = r.read().decode()
        except Exception:
            pass
        lags = re.findall(r"^cdc_consumer_lag\{[^}]*\} ([0-9.e+-]+)$", lag_text or "", re.M)
        batches = metric("cdc_batches_total") or 0
        # done when the lag gauge exists for the partitions and has been zero twice in a row (it refreshes every 15 s)
        if lags and sum(float(x) for x in lags) == 0 and batches > 0:
            drained += 1
        else:
            drained = 0
        if drained >= 2:
            break
        time.sleep(16)
    events = metric("cdc_events_total")
    errors = metric("cdc_errors_total") or 0
    dead = int(metric("cdc_dlq_total") or 0)
    indexed = int(metric("cdc_docs_indexed_total") or 0)
    cleanup_procs()
    counts_after = {i: client.count(index=i)["count"] for i in counts_before}
    parity = subprocess.run([PY, str(Path(__file__).with_name("parity.py")), "300"], cwd=ROOT, capture_output=True, text=True)
    # The topics still hold the deliberately malformed events published by the poison test: 3 per run of it.
    only_poison = dead % 3 == 0
    result(name, counts_after == counts_before and parity.returncode == 0 and errors == 0 and only_poison and drained >= 2,
           f"replayed {int(events or 0)} row events ({indexed} documents rewritten) in {round(time.time() - t0)}s, errors={int(errors)}, "
           f"dead letters={dead} (the intentionally published poison events), counts unchanged={counts_after == counts_before}, "
           f"parity on 900 sampled documents={'identical' if parity.returncode == 0 else 'DIFFERENT'}")
    if parity.returncode != 0:
        print(parity.stdout[-1500:])


if __name__ == "__main__":
    stop_main_service()
    try:
        for t in (t_es_outage, t_oracle_outage, t_crash, t_poison, t_replay):
            try:
                t()
            except Exception as exc:
                import traceback
                traceback.print_exc()
                result(t.__name__, False, f"harness error: {exc}")
            finally:
                cleanup_procs()
                run_reverts()
    finally:
        cleanup_procs()
        run_reverts()
        start_main_service()
    print("\n==================== SUMMARY")
    for name, ok, detail in RESULTS:
        print(f"{'PASS' if ok else 'FAIL'}  {name}  {detail}")
    sys.exit(0 if all(r[1] for r in RESULTS) else 1)
