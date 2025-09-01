from enum import Enum
from dataclasses import dataclass
from typing import Optional
import time


class OperationType(Enum):
    CREATE = 0
    UPDATE = 1
    DELETE = 2


@dataclass
class CRDTOperation:
    file_id: str
    operation_type: OperationType
    payload: bytes  # Can represent content, filename, metadata etc.
    logical_timestamp: int
    peer_id: str
    parent_hash: Optional[str] = None
