"""Command line: cdc-es-sync [--config config.yaml] run | check | reconcile | resync <index>

Exit codes: 0 success, 1 a check or comparison failed (or resync was interrupted), 2 configuration or preconditions.
"""
from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Sequence
from pathlib import Path

from .errors import ConfigError, ShutdownRequested
from .lifecycle import StopSignal, install_signal_handlers
from .observability.logs import setup_logging
from .settings import load_settings

log = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="cdc-es-sync", description=__doc__.splitlines()[0])
    parser.add_argument("--config", default=str(Path.cwd() / "config.yaml"), help="path of config.yaml")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("run", help="consume change events and keep Elasticsearch in step")
    sub.add_parser("check", help="verify Kafka, Elasticsearch, Oracle, the SQL, logging groups and mapping coverage")
    sub.add_parser("reconcile", help="compare document counts and amount totals between Oracle and Elasticsearch")
    rs = sub.add_parser("resync", help="rebuild every document of one index from Oracle")
    rs.add_argument("index")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        settings = load_settings(args.config)
        obs = settings.observability
        setup_logging(obs.log_level, obs.log_file)

        from .app import Application  # imported late: a config error is reported without loading the clients

        stop = StopSignal()
        install_signal_handlers(stop)
        app = Application(settings, stop)
    except ConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return 2

    if args.command == "check":
        results = app.check()
        for name, ok, detail in results:
            print(f"{'PASS' if ok else 'FAIL'}  {name}: {detail}")
        return 0 if all(r.ok for r in results) else 1
    if args.command == "reconcile":
        from .pipeline.reconcile import format_report
        rows, ok = app.reconcile()
        print(format_report(rows))
        return 0 if ok else 1
    if args.command == "resync":
        if args.index not in app.registry.indices:
            print(f"unknown index {args.index}; known: {sorted(app.registry.indices)}", file=sys.stderr)
            return 2
        try:
            total = app.resync(args.index)
        except ShutdownRequested:
            log.warning("resync interrupted")
            return 1
        print(f"resync {args.index}: {total} documents written")
        return 0
    return app.run()


if __name__ == "__main__":
    sys.exit(main())
