"""Process lifecycle: the stop signal every wait listens to, and the retry backoff."""
from __future__ import annotations

import random
import signal
import threading


class StopSignal:
    """Set by SIGINT/SIGTERM. Every wait in the program is interruptible through it."""

    def __init__(self) -> None:
        self._event = threading.Event()

    def set(self) -> None:
        self._event.set()

    def is_set(self) -> bool:
        return self._event.is_set()

    def wait(self, seconds: float) -> bool:
        """Sleeps; returns True when a stop was requested."""
        return self._event.wait(seconds)


def install_signal_handlers(stop: StopSignal) -> None:
    """SIGINT, SIGTERM (Docker stop) and SIGBREAK (Windows Ctrl+Break) all request a clean stop."""
    for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
        if hasattr(signal, name):
            signal.signal(getattr(signal, name), lambda *_: stop.set())


def backoff_delay(attempt: int, cap: float) -> float:
    """Exponential backoff from 0.5 s, capped, with +-30 % jitter so retries of several clients spread out."""
    return min(cap, 0.5 * (2 ** min(attempt, 10))) * random.uniform(0.7, 1.3)
