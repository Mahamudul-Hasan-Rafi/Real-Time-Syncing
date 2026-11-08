"""JSON log lines on the console and, optionally, in a rotated file."""
from __future__ import annotations

import json
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path


class JsonFormatter(logging.Formatter):
    """One JSON object per line. `extra={"ctx": {...}}` adds fields to the line."""

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


def setup_logging(level: str, log_file: Path | None) -> None:
    root = logging.getLogger()
    root.setLevel(level.upper())
    for h in list(root.handlers):
        root.removeHandler(h)
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(RotatingFileHandler(log_file, maxBytes=20_000_000, backupCount=5, encoding="utf-8"))
    for h in handlers:
        h.setFormatter(JsonFormatter())
        root.addHandler(h)
    logging.getLogger("elastic_transport").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
