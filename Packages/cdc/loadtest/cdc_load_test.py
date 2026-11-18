"""CDC load test: no-op updates on the captured EMOB tables, paced over a fixed duration.

Each update is `UPDATE <table> SET <col> = <col> WHERE ROWID = :r`. Oracle writes full redo and Debezium emits a
change event, but no stored value changes, so nothing has to be reverted. Tables with enabled triggers are refused,
because a trigger could change real data.

Credentials come from Packages/cdc/elastic/.env (ORACLE_USER / ORACLE_PASSWORD), the host from Packages/cdc/.env.

  python cdc_load_test.py --dry-run                 # pre-flight only: row counts, chosen columns, triggers
  python cdc_load_test.py --total 10000 --minutes 7
"""
import argparse
import itertools
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import oracledb

TABLES = [
    "MB_ACCOUNT_MST", "UNION_INFO", "UPAZILA_INFO", "DISTRICT_INFO", "MB_ACCOUNT_DAILY_BALANCE", "MB_ACCOUNT_OPERATOR",
    "MB_ADDRESS_MST", "MB_CUSTOMER_MST", "MB_DOCUMENT_MST", "MB_PRODUCT_MST", "STCODMAS", "ST_AGENT_POINT",
    "ST_OPERATING_INSTRUCTION", "ST_POINT_CATEGORY", "ST_SECTOR_CODE", "ST_TRANSACTION_CODE", "NOMINEE_MST",
]
CDC_DIR = Path(__file__).resolve().parent.parent


def read_env(path):
    env = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    return env


def connect(service):
    cdc, elastic = read_env(CDC_DIR / ".env"), read_env(CDC_DIR / "elastic" / ".env")
    dsn = f"{cdc['ORACLE_HOST']}:{cdc.get('ORACLE_PORT', '1521')}/{service}"
    return oracledb.connect(user=elastic["ORACLE_USER"], password=elastic["ORACLE_PASSWORD"], dsn=dsn), dsn


def preflight(cur, schema, sample):
    """Per table: enabled triggers, a non-key column to update, and up to `sample` ROWIDs."""
    plan = {}
    for t in TABLES:
        cur.execute("SELECT COUNT(*) FROM all_triggers WHERE table_owner = :o AND table_name = :t AND status = 'ENABLED'", o=schema, t=t)
        triggers = cur.fetchone()[0]
        cur.execute("""
            SELECT c.column_name FROM all_tab_cols c
             WHERE c.owner = :o AND c.table_name = :t
               AND c.data_type IN ('VARCHAR2','CHAR','NUMBER','DATE','NVARCHAR2')
               AND c.column_name NOT IN (SELECT cc.column_name FROM all_cons_columns cc JOIN all_constraints k
                                           ON k.owner = cc.owner AND k.constraint_name = cc.constraint_name
                                          WHERE k.owner = :o AND k.table_name = :t AND k.constraint_type = 'P')
               AND c.virtual_column = 'NO' AND c.hidden_column = 'NO'
             ORDER BY c.column_id FETCH FIRST 1 ROWS ONLY""", o=schema, t=t)
        row = cur.fetchone()
        column = row[0] if row else None
        cur.execute(f'SELECT ROWIDTOCHAR(ROWID) FROM "{schema}"."{t}" WHERE ROWNUM <= :n', n=sample)
        rowids = [r[0] for r in cur.fetchall()]
        plan[t] = {"triggers": triggers, "column": column, "rows_sampled": len(rowids), "rowids": rowids}
    return plan


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--total", type=int, default=10000)
    ap.add_argument("--minutes", type=float, default=7)
    ap.add_argument("--batch", type=int, default=20, help="updates per transaction")
    ap.add_argument("--service", default="orclpdb1", help="service of the PDB that holds the schema")
    ap.add_argument("--schema", default="EMOB")
    ap.add_argument("--sample", type=int, default=2000, help="max distinct rows used per table")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    conn, dsn = connect(a.service)
    cur = conn.cursor()
    plan = preflight(cur, a.schema, a.sample)
    print(f"connected to {dsn} as {conn.username}\n")
    print(f"{'table':28} {'triggers':>8} {'rows':>6}  no-op column")
    for t, p in plan.items():
        print(f"{t:28} {p['triggers']:>8} {p['rows_sampled']:>6}  {p['column']}")

    blocked = [t for t, p in plan.items() if p["triggers"]]
    usable = [t for t, p in plan.items() if p["column"] and p["rowids"] and not p["triggers"]]
    skipped = [t for t in TABLES if t not in usable and t not in blocked]
    if blocked:
        raise SystemExit(f"\nREFUSED: enabled triggers on {blocked}; a no-op update would fire them.")
    if skipped:
        print(f"\nskipped (no rows or no updatable column): {skipped}")
    if a.dry_run:
        return

    batches = (a.total + a.batch - 1) // a.batch
    interval = a.minutes * 60 / batches
    rows = {t: itertools.cycle(plan[t]["rowids"]) for t in usable}
    order = itertools.cycle(usable)
    per_table = dict.fromkeys(usable, 0)
    print(f"\n{a.total} updates in {batches} transactions of {a.batch}, one every {interval:.2f}s, over {a.minutes} min")

    start = time.time()
    started_at = datetime.now(timezone.utc).isoformat()
    done = 0
    last_report = start
    for i in range(batches):
        t = next(order)
        n = min(a.batch, a.total - done)
        col = plan[t]["column"]
        cur.executemany(f'UPDATE "{a.schema}"."{t}" SET "{col}" = "{col}" WHERE ROWID = CHARTOROWID(:1)', [[next(rows[t])] for _ in range(n)])
        conn.commit()
        done += n
        per_table[t] += n
        now = time.time()
        if now - last_report >= 30 or done == a.total:
            print(f"  {done:>6}/{a.total}  {now - start:6.0f}s  {done / (now - start):6.1f} updates/s", flush=True)
            last_report = now
        sleep = start + (i + 1) * interval - time.time()
        if sleep > 0:
            time.sleep(sleep)

    summary = {"started_at": started_at, "ended_at": datetime.now(timezone.utc).isoformat(), "seconds": round(time.time() - start, 1),
               "updates": done, "transactions": batches, "per_table": per_table}
    print(json.dumps(summary, indent=2))
    (Path(__file__).parent / "last_run.json").write_text(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
