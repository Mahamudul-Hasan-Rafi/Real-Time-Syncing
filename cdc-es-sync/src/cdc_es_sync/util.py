from __future__ import annotations

import json
import logging
import random
import threading


class ShutdownRequested(Exception):
    pass


class Stop:
    """Set by SIGINT/SIGTERM. Every wait in the program is interruptible through it."""

    def __init__(self):
        self.event = threading.Event()

    def set(self):
        self.event.set()

    def is_set(self) -> bool:
        return self.event.is_set()

    def wait(self, seconds: float) -> bool:
        """Sleeps; returns True when a stop was requested."""
        return self.event.wait(seconds)


def backoff_delay(attempt: int, cap: float) -> float:
    return min(cap, 0.5 * (2 ** min(attempt, 10))) * random.uniform(0.7, 1.3)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            entry["exc"] = self.formatException(record.exc_info)
        extra = getattr(record, "ctx", None)
        if extra:
            entry.update(extra)
        return json.dumps(entry, default=str)


def setup_logging(level: str, log_file: str | None):
    root = logging.getLogger()
    root.setLevel(level.upper())
    for h in list(root.handlers):
        root.removeHandler(h)
    handlers = [logging.StreamHandler()]
    if log_file:
        from logging.handlers import RotatingFileHandler
        handlers.append(RotatingFileHandler(log_file, maxBytes=20_000_000, backupCount=5, encoding="utf-8"))
    for h in handlers:
        h.setFormatter(JsonFormatter())
        root.addHandler(h)
    logging.getLogger("elastic_transport").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
