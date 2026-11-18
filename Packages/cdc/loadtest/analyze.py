"""Reads last_run.json and reports how the CDC pipeline handled the load, from Prometheus.

  python analyze.py [--prometheus http://localhost:9090] [--drain 300]

--drain: seconds after the load ends that are still counted, so events still in flight are included.
"""
import argparse
import json
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path

TOPICS = 'topic=~"emob\\\\.EMOB\\\\..*"'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--prometheus", default="http://localhost:9090")
    ap.add_argument("--drain", type=int, default=300)
    a = ap.parse_args()
    run = json.loads((Path(__file__).parent / "last_run.json").read_text())
    t0 = datetime.fromisoformat(run["started_at"]).timestamp()
    t1 = datetime.fromisoformat(run["ended_at"]).timestamp()
    t_end = t1 + a.drain
    win = f"{int(t_end - t0) + 30}s"

    def query(expr, at):
        url = f"{a.prometheus}/api/v1/query?" + urllib.parse.urlencode({"query": expr, "time": at})
        return json.load(urllib.request.urlopen(url, timeout=30))["data"]["result"]

    def scalar(expr, at=t_end):
        r = query(expr, at)
        return float(r[0]["value"][1]) if r else None

    def series(expr, step=15):
        url = f"{a.prometheus}/api/v1/query_range?" + urllib.parse.urlencode({"query": expr, "start": t0 - 60, "end": t_end, "step": step})
        r = json.load(urllib.request.urlopen(url, timeout=30))["data"]["result"]
        return [(float(t), float(v)) for t, v in r[0]["values"]] if r else []

    def delta(expr):
        before, after = scalar(expr, t0 - 30), scalar(expr, t_end)
        return None if before is None or after is None else after - before

    def settled(expr):
        """Seconds after the load ended until the series reached its final value."""
        pts = series(expr)
        if not pts:
            return None
        final = pts[-1][1]
        first = next(t for t, v in pts if v == final)
        return max(0.0, first - t1)

    dml = 'max(debezium_metrics_totalcaptureddmlcount{context="streaming"})'
    produced = f"sum(kafka_topic_partition_current_offset{{{TOPICS}}})"
    consumed = "sum(cdc_events_total) + sum(cdc_events_skipped_total)"
    r = {
        "load": {"updates": run["updates"], "transactions": run["transactions"], "seconds": run["seconds"], "updates_per_s": round(run["updates"] / run["seconds"], 1)},
        "debezium": {
            "dml_captured": delta(dml),
            "max_source_lag_s": scalar(f'max_over_time(debezium_metrics_millisecondsbehindsource{{context="streaming"}}[{win}]) / 1000'),
            "avg_source_lag_s": scalar(f'avg_over_time((debezium_metrics_millisecondsbehindsource{{context="streaming"}} >= 0)[{win}:15s]) / 1000'),
            "max_batch_processing_ms": scalar(f'max_over_time(debezium_metrics_lastbatchprocessingtimeinmilliseconds{{context="streaming"}}[{win}])'),
            "min_queue_free": scalar(f'min_over_time(debezium_metrics_queueremainingcapacity{{context="streaming"}}[{win}])'),
            "errors": delta('max(debezium_metrics_errorcount{context="streaming"})'),
            "settled_after_load_s": settled(dml),
        },
        "kafka": {
            "messages_produced": delta(produced),
            "per_topic": {x["metric"]["topic"].split(".")[-1]: None for x in query(f"sum by (topic) (kafka_topic_partition_current_offset{{{TOPICS}}})", t_end)},
            "settled_after_load_s": settled(produced),
        },
        "cdc_es_sync": {
            "events_read": delta("sum(cdc_events_total)"),
            "events_skipped": {x["metric"]["reason"]: None for x in query("cdc_events_skipped_total", t_end)},
            "docs_updated": delta("sum(cdc_docs_updated_total)"),
            "errors": delta("sum(cdc_errors_total)"),
            "dead_lettered": delta("sum(cdc_dlq_total)"),
            "max_consumer_lag": scalar(f"max_over_time(sum(cdc_consumer_lag)[{win}:15s])"),
            "settled_after_load_s": settled(consumed),
        },
        "oracle": {
            "archive_bytes_last_hour_at_end": scalar("oracledb_archive_log_bytes_last_hour"),
            "redo_switches_last_hour_at_end": scalar("oracledb_redo_switches_last_hour"),
            "fra_used_pct_at_end": scalar("100 * oracledb_fra_used_bytes / oracledb_fra_limit_bytes"),
        },
    }
    for topic in r["kafka"]["per_topic"]:
        r["kafka"]["per_topic"][topic] = delta(f'sum(kafka_topic_partition_current_offset{{topic="emob.EMOB.{topic}"}})')
    for reason in r["cdc_es_sync"]["events_skipped"]:
        r["cdc_es_sync"]["events_skipped"][reason] = delta(f'sum(cdc_events_skipped_total{{reason="{reason}"}})')
    print(json.dumps(r, indent=2))


if __name__ == "__main__":
    main()
