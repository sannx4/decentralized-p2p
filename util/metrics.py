import time
import threading
from typing import Dict, Any


class _CounterStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: Dict[str, int] = {}

    def inc(self, name: str, value: int = 1) -> None:
        with self._lock:
            self._counters[name] = self._counters.get(name, 0) + value

    def get(self, name: str) -> int:
        with self._lock:
            return self._counters.get(name, 0)

    def snapshot(self) -> Dict[str, int]:
        with self._lock:
            return dict(self._counters)


COUNTERS = _CounterStore()


def incr(name: str, value: int = 1) -> None:
    COUNTERS.inc(name, value)


def snapshot() -> Dict[str, int]:
    return COUNTERS.snapshot()


def now_ms() -> int:
    return int(time.time() * 1000)
