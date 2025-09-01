import json
import sys
from typing import Any, Dict
from util.metrics import now_ms


def log(event: str, **fields: Dict[str, Any]) -> None:
    record = {"ts_ms": now_ms(), "event": event}
    record.update(fields)
    print(json.dumps(record, separators=(",", ":")), file=sys.stdout, flush=True)
