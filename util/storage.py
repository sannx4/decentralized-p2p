import os
import json
from typing import Any, Optional


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def save_snapshot(path: str, state: Any) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, separators=(",", ":"), ensure_ascii=False)


def load_snapshot(path: str) -> Optional[Any]:
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)