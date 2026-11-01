"""Read-only completeness check of every lookup query in config.yaml.

For real rows of each child/dimension table it works out which documents depend on that row (from the
documents already in Elasticsearch, a source independent of the lookup SQL) and checks that the lookup
query returns every one of them. A document the lookup misses would go stale after a change.

    python tests/live/lookup_crosscheck.py [rows per table]
"""
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import elasticsearch as es
from elasticsearch import helpers

from cdc_es_sync.config import load_config
from cdc_es_sync.mapping import Registry, norm
from cdc_es_sync.oracle_reader import OracleReader

ROOT = Path(__file__).resolve().parents[2]
cfg = load_config(ROOT / "config.yaml")
reg = Registry.from_config(cfg)
tz = ZoneInfo(cfg["oracle"]["timezone"])
client = es.Elasticsearch(cfg["elasticsearch"]["targets"]["default"]["hosts"], request_timeout=300)
reader = OracleReader(cfg["oracle"])
N = int(sys.argv[1]) if len(sys.argv) > 1 else 150


def sql_rows(sql, binds=None):
    with reader.cursor() as cur:
        cur.execute(sql, binds or {})
        return cur.fetchall()


def sample(table, cols, where=""):
    return sql_rows(f"SELECT * FROM (SELECT {cols} FROM emob.{table} {where} ORDER BY dbms_random.value) WHERE rownum <= {N}")


def lookup(impact, params):
    ix = reg.indices[impact.index]
    out = set()
    for chunk in reader.lookup_chunks(impact.sql, params, 5000):
        for values in chunk:
            out.add(tuple(norm(v, t) for v, t in zip(values, ix.key_types)))
    return out


def iso_to_local(v):
    return datetime.fromisoformat(v.replace("Z", "+00:00")).astimezone(tz).replace(tzinfo=None)


# ------------------------------------------------------------------------------- load documents
print("loading customer_data and ac_balance_data from Elasticsearch ...", flush=True)
cust_fields = ["ac_no", "cust_no", "ac_id", "ac_point_id", "agent_point_id", "ac_type_id", "opr_ins", "occupation_code",
               "last_education", "point_category", "doc", "address", "operator", "nominee"]
C = []
for h in helpers.scan(client, index="customer_data", query={"query": {"match_all": {}}}, _source=cust_fields, size=5000):
    C.append(h["_source"])
bal_fields = ["ac_id", "balance_date", "cust_no", "point_name", "product", "occupation", "education", "district",
              "point_category"]
B = []
for h in helpers.scan(client, index="ac_balance_data", query={"query": {"match_all": {}}}, _source=bal_fields, size=5000):
    s = h["_source"]
    B.append(s)
print(f"  customer docs={len(C)} balance docs={len(B)}", flush=True)
tran_fields = ["tran_id", "ac_id", "cust_no", "ac_type_id", "tran_code", "sector_code", "tran_point_id", "ac_point_id",
               "ac_point_category_id", "point_category", "agent_id", "occupation", "point_district", "ac_point_district",
               "point_upazilla", "point_union"]
T = [h["_source"] for h in helpers.scan(client, index="transaction_history_", query={"query": {"match_all": {}}},
                                        _source=tran_fields, size=5000)]
print(f"  transaction docs={len(T)}", flush=True)

# inverted indexes over the documents
c_by = defaultdict(lambda: defaultdict(set))       # field -> value -> {ac_no}
for d in C:
    key = (d["ac_no"],)
    for f in ("cust_no", "ac_id", "ac_type_id", "opr_ins", "occupation_code", "last_education", "point_category"):
        if d.get(f) is not None:
            c_by[f][d[f]].add(key)
    for f in ("ac_point_id", "agent_point_id"):
        if d.get(f) is not None:
            c_by["point"][d[f]].add(key)
    for x in d.get("doc") or []:
        c_by["document_id"][x["document_id"]].add(key)
    for x in d.get("address") or []:
        c_by["address_id"][x["address_id"]].add(key)
        c_by["district_name"][x.get("district")].add(key)
        c_by["upazila_name"][(x.get("district"), x.get("upazilla"))].add(key)
        c_by["union_name"][(x.get("district"), x.get("upazilla"), x.get("union"))].add(key)
    for x in d.get("operator") or []:
        c_by["operator_row"][(x["ac_id"], x["cust_no"])].add(key)
        c_by["operator_cust"][x["cust_no"]].add(key)
    for x in d.get("nominee") or []:
        c_by["nominee_id"][x["nominee_id"]].add(key)

b_by = defaultdict(lambda: defaultdict(set))
for d in B:
    key = (d["ac_id"], iso_to_local(d["balance_date"]))
    for f in ("ac_id", "cust_no", "point_name", "product", "occupation", "education", "district", "point_category"):
        if d.get(f) is not None:
            b_by[f][d[f]].add(key)

# ------------------------------------------------------------------------------- impact lookup by id
impacts = {}
for table, lst in reg.tables.items():
    for imp in lst:
        impacts[imp.id] = imp


def find(table, index, n=0):
    return [i for i in reg.tables[table] if i.index == index][n]


results = []          # (label, rows checked, rows with missing docs, total expected, total missing, extras)


def check(label, imp_list, cases):
    """cases: list of (params, expected key set)."""
    missing_rows = exp_total = miss_total = extras = 0
    examples = []
    for params, expected in cases:
        got = set()
        for imp in imp_list:
            got |= lookup(imp, params)
        miss = expected - got
        exp_total += len(expected)
        miss_total += len(miss)
        extras += len(got - expected)
        if miss:
            missing_rows += 1
            if len(examples) < 3:
                examples.append((params, sorted(miss, key=repr)[:3]))
    results.append((label, len(cases), missing_rows, exp_total, miss_total, extras, examples))


# ------------------------------------------------------------------------------- customer_data
check("customer_data <- MB_DOCUMENT_MST", [find("MB_DOCUMENT_MST", "customer_data")],
      [({"CUST_NO": int(c)}, c_by["document_id"].get(int(d), set())) for d, c in sample("mb_document_mst", "document_id, cust_no")])
check("customer_data <- MB_ADDRESS_MST", [find("MB_ADDRESS_MST", "customer_data")],
      [({"REF_NO": int(r)}, c_by["address_id"].get(int(a), set())) for a, r in sample("mb_address_mst", "address_id, ref_no")])
check("customer_data <- NOMINEE_MST", [find("NOMINEE_MST", "customer_data")],
      [({"AC_ID": int(a)}, c_by["nominee_id"].get(int(n), set())) for n, a in sample("nominee_mst", "nominee_id, ac_id")])
check("customer_data <- MB_ACCOUNT_OPERATOR", [find("MB_ACCOUNT_OPERATOR", "customer_data")],
      [({"AC_ID": int(a)}, c_by["operator_row"].get((int(a), int(c)), set())) for a, c in sample("mb_account_operator", "ac_id, cust_no")])
check("customer_data <- MB_CUSTOMER_MST (own accounts + operator)",
      [find("MB_CUSTOMER_MST", "customer_data", 0), find("MB_CUSTOMER_MST", "customer_data", 1)],
      [({"CUST_NO": int(c)}, c_by["cust_no"].get(int(c), set()) | c_by["operator_cust"].get(int(c), set()))
       for (c,) in sample("mb_customer_mst", "cust_no")])
check("customer_data <- MB_PRODUCT_MST", [find("MB_PRODUCT_MST", "customer_data")],
      [({"AC_TYPE_ID": int(t)}, c_by["ac_type_id"].get(int(t), set())) for (t,) in sample("mb_product_mst", "ac_type_id")])
check("customer_data <- ST_AGENT_POINT", [find("ST_AGENT_POINT", "customer_data")],
      [({"POINT_ID": int(p)}, c_by["point"].get(int(p), set())) for (p,) in sample("st_agent_point", "point_id")])
cat_names = {int(i): n for i, n in sql_rows("SELECT point_category_id, category_name FROM emob.st_point_category")}
check("customer_data <- ST_POINT_CATEGORY", [find("ST_POINT_CATEGORY", "customer_data")],
      [({"POINT_CATEGORY_ID": i}, c_by["point_category"].get(n, set())) for i, n in cat_names.items()])
dist_names = {d: n for d, n in sql_rows("SELECT district_code, district_name FROM emob.district_info")}
check("customer_data <- DISTRICT_INFO", [find("DISTRICT_INFO", "customer_data")],
      [({"DISTRICT_CODE": d}, c_by["district_name"].get(n, set())) for d, n in dist_names.items()])
upz_all = sql_rows("SELECT district_code, upazila_code, upazila_name FROM emob.upazila_info")
# The documents only carry names. Two upazilas can share a name (Chandpur has two "Faridganj"), so only
# rows whose name is unique inside the district can be checked by name.
upz_count = defaultdict(int)
for d, u, n in upz_all:
    upz_count[(d, n)] += 1
upz = [r for r in upz_all if upz_count[(r[0], r[2])] == 1]
print(f"  upazilas checkable by name: {len(upz)} of {len(upz_all)}")
check("customer_data <- UPAZILA_INFO", [find("UPAZILA_INFO", "customer_data")],
      [({"DISTRICT_CODE": d, "UPAZILA_CODE": u}, c_by["upazila_name"].get((dist_names.get(d), n), set())) for d, u, n in upz[:N]])
uni_all = sql_rows("SELECT u.district_code, u.upazila_code, u.union_code, u.union_name, p.upazila_name "
                   "FROM emob.union_info u JOIN emob.upazila_info p ON p.district_code=u.district_code AND p.upazila_code=u.upazila_code")
uni_count = defaultdict(int)
for d, u, c, n, un in uni_all:
    uni_count[(d, un, n)] += 1
upz_names = defaultdict(int)
for d, u, n in upz_all:
    upz_names[(d, n)] += 1
uni = [r for r in uni_all if uni_count[(r[0], r[4], r[3])] == 1 and upz_names[(r[0], r[4])] == 1][:N]
print(f"  unions checkable by name: {len(uni)} sampled")
check("customer_data <- UNION_INFO", [find("UNION_INFO", "customer_data")],
      [({"DISTRICT_CODE": d, "UPAZILA_CODE": u, "UNION_CODE": c}, c_by["union_name"].get((dist_names.get(d), un, n), set()))
       for d, u, c, n, un in uni])
check("customer_data <- STCODMAS (occupation)", [find("STCODMAS", "customer_data")],
      [({"HARCOD": "OCC", "SOFCOD": s}, c_by["occupation_code"].get(s, set())) for (s,) in sql_rows("SELECT sofcod FROM emob.stcodmas WHERE harcod='OCC'")])
check("customer_data <- STCODMAS (education)", [find("STCODMAS", "customer_data")],
      [({"HARCOD": "EDT", "SOFCOD": s}, c_by["last_education"].get(s, set())) for (s,) in sql_rows("SELECT sofcod FROM emob.stcodmas WHERE harcod='EDT'")])
check("customer_data <- ST_OPERATING_INSTRUCTION", [find("ST_OPERATING_INSTRUCTION", "customer_data")],
      [({"OPR_INS": o}, c_by["opr_ins"].get(o, set())) for (o,) in sql_rows("SELECT opr_ins FROM emob.st_operating_instruction")])
check("customer_data <- MB_ACCOUNT_MST (direct key)", [],
      [({}, set()) for _ in range(0)])

# ------------------------------------------------------------------------------- ac_balance_data
check("ac_balance_data <- MB_ACCOUNT_MST", [find("MB_ACCOUNT_MST", "ac_balance_data")],
      [({"AC_ID": int(a)}, b_by["ac_id"].get(int(a), set())) for (a,) in sample("mb_account_mst", "ac_id")])
check("ac_balance_data <- MB_CUSTOMER_MST", [find("MB_CUSTOMER_MST", "ac_balance_data")],
      [({"CUST_NO": int(c)}, b_by["cust_no"].get(int(c), set())) for (c,) in sample("mb_customer_mst", "cust_no")])
prod = sql_rows("SELECT ac_type_id, full_desc FROM emob.mb_product_mst")
check("ac_balance_data <- MB_PRODUCT_MST", [find("MB_PRODUCT_MST", "ac_balance_data")],
      [({"AC_TYPE_ID": int(t)}, b_by["product"].get(n, set())) for t, n in prod])
check("ac_balance_data <- ST_AGENT_POINT", [find("ST_AGENT_POINT", "ac_balance_data")],
      [({"POINT_ID": int(p)}, b_by["point_name"].get(n, set())) for p, n in sample(
          "st_agent_point", "point_id, point_name",
          "WHERE point_id IN (SELECT a.point_id FROM emob.mb_account_mst a JOIN emob.mb_account_daily_balance b ON b.ac_id = a.ac_id)")])
check("ac_balance_data <- ST_POINT_CATEGORY", [find("ST_POINT_CATEGORY", "ac_balance_data")],
      [({"POINT_CATEGORY_ID": i}, b_by["point_category"].get(n, set())) for i, n in cat_names.items()])
check("ac_balance_data <- DISTRICT_INFO", [find("DISTRICT_INFO", "ac_balance_data")],
      [({"DISTRICT_CODE": d}, b_by["district"].get(n, set())) for d, n in dist_names.items()])
occ = {s: n for s, n in sql_rows("SELECT sofcod, coddes FROM emob.stcodmas WHERE harcod='OCC'")}
edu = {s: n for s, n in sql_rows("SELECT sofcod, coddes FROM emob.stcodmas WHERE harcod='EDT'")}
check("ac_balance_data <- STCODMAS (occupation)", [find("STCODMAS", "ac_balance_data")],
      [({"HARCOD": "OCC", "SOFCOD": s}, b_by["occupation"].get(n, set())) for s, n in occ.items()])
check("ac_balance_data <- STCODMAS (education)", [find("STCODMAS", "ac_balance_data")],
      [({"HARCOD": "EDT", "SOFCOD": s}, b_by["education"].get(n, set())) for s, n in edu.items()])

# ------------------------------------------------------------------------------- transaction_history
def code_of(text):
    """'01-Dhaka' -> '01' (the point fields hold CODE-NAME)."""
    return None if text is None else str(text).split("-", 1)[0]


t_by = defaultdict(lambda: defaultdict(set))
for d in T:
    key = (int(d["tran_id"]),)
    for f in ("ac_id", "cust_no", "ac_type_id", "tran_code", "tran_point_id", "ac_point_id", "ac_point_category_id",
              "agent_id", "occupation", "sector_code", "point_category"):
        if d.get(f) is not None:
            t_by[f][d[f]].add(key)
    if d.get("point_district") is not None:
        t_by["point_district"][code_of(d["point_district"])].add(key)
        t_by["point_upazilla"][(code_of(d["point_district"]), code_of(d.get("point_upazilla")))].add(key)
        t_by["point_union"][(code_of(d["point_district"]), code_of(d.get("point_upazilla")), code_of(d.get("point_union")))].add(key)
    if d.get("ac_point_district") is not None:
        t_by["ac_point_district"][code_of(d["ac_point_district"])].add(key)

TX = "transaction_history"
check("transaction_history <- MB_ACCOUNT_MST", [find("MB_ACCOUNT_MST", TX)],
      [({"AC_ID": int(a)}, t_by["ac_id"].get(int(a), set())) for (a,) in sample("mb_account_mst", "ac_id")])
check("transaction_history <- MB_CUSTOMER_MST (own + agent name)",
      [find("MB_CUSTOMER_MST", TX, 0), find("MB_CUSTOMER_MST", TX, 1)],
      [({"CUST_NO": int(c)}, t_by["cust_no"].get(int(c), set()) | t_by["agent_id"].get(int(c), set()))
       for (c,) in sample("mb_customer_mst", "cust_no")])
check("transaction_history <- MB_PRODUCT_MST", [find("MB_PRODUCT_MST", TX)],
      [({"AC_TYPE_ID": int(t)}, t_by["ac_type_id"].get(int(t), set())) for (t,) in sql_rows("SELECT ac_type_id FROM emob.mb_product_mst")])
check("transaction_history <- ST_TRANSACTION_CODE", [find("ST_TRANSACTION_CODE", TX)],
      [({"TRAN_CODE": c}, t_by["tran_code"].get(c, set())) for (c,) in sql_rows("SELECT tran_code FROM emob.st_transaction_code")])
check("transaction_history <- ST_SECTOR_CODE", [find("ST_SECTOR_CODE", TX)],
      [({"SRLNO": s}, t_by["sector_code"].get(f"{sc}-{ds}", set()))
       for s, sc, ds in sql_rows("SELECT srlno, sector_code, sector_desc FROM emob.st_sector_code")])
check("transaction_history <- ST_AGENT_POINT (account point + transaction point)",
      [find("ST_AGENT_POINT", TX, 0), find("ST_AGENT_POINT", TX, 1)],
      [({"POINT_ID": int(p)}, t_by["ac_point_id"].get(p, set()) | t_by["tran_point_id"].get(p, set()))
       for (p,) in sample("st_agent_point", "point_id")])
unique_cat = {i: n for i, n in cat_names.items() if sum(1 for m in cat_names.values() if m == n) == 1}
check("transaction_history <- ST_POINT_CATEGORY", [find("ST_POINT_CATEGORY", TX, 0), find("ST_POINT_CATEGORY", TX, 1)],
      [({"POINT_CATEGORY_ID": i}, t_by["ac_point_category_id"].get(i, set()) | t_by["point_category"].get(n, set()))
       for i, n in unique_cat.items()])
check("transaction_history <- DISTRICT_INFO", [find("DISTRICT_INFO", TX, 0), find("DISTRICT_INFO", TX, 1)],
      [({"DISTRICT_CODE": d}, t_by["ac_point_district"].get(d, set()) | t_by["point_district"].get(d, set()))
       for d in dist_names])
check("transaction_history <- UPAZILA_INFO", [find("UPAZILA_INFO", TX)],
      [({"DISTRICT_CODE": d, "UPAZILA_CODE": u}, t_by["point_upazilla"].get((d, u), set())) for d, u, _ in upz_all])
check("transaction_history <- UNION_INFO", [find("UNION_INFO", TX)],
      [({"DISTRICT_CODE": d, "UPAZILA_CODE": u, "UNION_CODE": c}, t_by["point_union"].get((d, u, c), set()))
       for d, u, c, _, _ in uni_all])
occ_unique = {s: n for s, n in occ.items() if sum(1 for m in occ.values() if m == n) == 1}
check("transaction_history <- STCODMAS (occupation)", [find("STCODMAS", TX)],
      [({"HARCOD": "OCC", "SOFCOD": s}, t_by["occupation"].get(n, set())) for s, n in occ_unique.items()])

# ------------------------------------------------------------------------------- report
print()
print(f"{'lookup':58} {'rows':>5} {'rows missing':>12} {'docs expected':>13} {'docs missing':>12} {'extra docs':>10}")
bad = 0
for label, rows, miss_rows, exp, miss, extra, examples in results:
    if rows == 0:
        continue
    flag = "" if miss == 0 else "   <-- INCOMPLETE"
    bad += 1 if miss else 0
    print(f"{label:58} {rows:5} {miss_rows:12} {exp:13} {miss:12} {extra:10}{flag}")
    for e in examples:
        print("      e.g.", e)
reader.close()
print("\nRESULT:", "every lookup returns every dependent document" if not bad else f"{bad} lookup(s) incomplete")
sys.exit(1 if bad else 0)
