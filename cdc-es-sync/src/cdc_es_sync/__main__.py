"""Command line: python -m cdc_es_sync [--config config.yaml] run | check | resync <index> | reconcile"""
from __future__ import annotations

import argparse
import logging
import signal
import sys
from pathlib import Path

from .config import ConfigError, load_config
from .util import ShutdownRequested, Stop, setup_logging


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="cdc_es_sync")
    parser.add_argument("--config", default=str(Path.cwd() / "config.yaml"))
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("run", help="consume change events and keep Elasticsearch in step")
    sub.add_parser("check", help="verify Kafka, Elasticsearch, Oracle, the SQL, logging groups and mapping coverage")
    sub.add_parser("reconcile", help="compare document counts and amount totals between Oracle and Elasticsearch")
    rs = sub.add_parser("resync", help="rebuild every document of one index from Oracle")
    rs.add_argument("index")
    args = parser.parse_args(argv)

    try:
        cfg = load_config(args.config)
    except ConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return 2
    obs = cfg.get("observability", {})
    base = Path(cfg["_base"])
    log_file = None
    if obs.get("log_file"):
        log_file = str(base / obs["log_file"])
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
    setup_logging(obs.get("log_level", "INFO"), log_file)

    from .service import Service

    stop = Stop()
    for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
        if hasattr(signal, name):
            signal.signal(getattr(signal, name), lambda *_: stop.set())
    service = Service(cfg, stop)

    if args.command == "check":
        results = service.checks()
        for name, ok, detail in results:
            print(f"{'PASS' if ok else 'FAIL'}  {name}: {detail}")
        return 0 if all(r[1] for r in results) else 1
    if args.command == "reconcile":
        from .reconcile import format_report, reconcile
        rows, ok = reconcile(service.registry, service.reader, service.writer)
        print(format_report(rows))
        return 0 if ok else 1
    if args.command == "resync":
        if args.index not in service.registry.indices:
            print(f"unknown index {args.index}; known: {sorted(service.registry.indices)}", file=sys.stderr)
            return 2
        try:
            total = service.engine.resync(args.index)
        except ShutdownRequested:
            logging.getLogger("main").warning("resync interrupted")
            return 1
        print(f"resync {args.index}: {total} documents written")
        return 0
    try:
        service.run()
    except SystemExit as exc:
        return int(exc.code or 0)
    return 0


if __name__ == "__main__":
    sys.exit(main())
