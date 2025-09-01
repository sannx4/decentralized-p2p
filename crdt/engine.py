import hashlib
from typing import Dict, List
from crdt.types import CRDTOperation, OperationType
from p2p.proto import crdt_pb2


class CRDTEngine:
    """
    Minimal CRDT engine that supports the methods the gossip layer calls:
      - version_vector() -> Dict[str,int]
      - iter_ops_since(vv) -> Iterable[CRDTOperation]
      - apply_ops(list[CRDTOperation]) -> tuple[count, vv]
    """

    def __init__(self, peer_id: str):
        self.peer_id = peer_id
        self.op_log: List[CRDTOperation] = []
        self._seen_hashes: set[str] = set()
        self._vv: Dict[str, int] = {}      # peer_id -> max counter seen
        self._op_count: int = 0            # our local counter

    # ----- required by gossip -----
    def version_vector(self) -> Dict[str, int]:
        return dict(self._vv)

    def iter_ops_since(self, vv: Dict[str, int]):
        want = vv or {}
        for op in self.op_log:
            if op.logical_timestamp > want.get(op.peer_id, 0):
                yield op

    def apply_ops(self, ops: List[CRDTOperation]):
        applied = 0
        for op in ops:
            h = self._generate_hash(op)
            if h in self._seen_hashes:
                continue
            self._seen_hashes.add(h)
            self.op_log.append(op)
            self._vv[op.peer_id] = max(self._vv.get(op.peer_id, 0), int(op.logical_timestamp))
            applied += 1
        return applied, self.version_vector()

    # ----- local op API (used by LocalOpsHelper) -----
    def apply_local_operation(self, op_type: OperationType, file_id: str, payload: bytes) -> CRDTOperation:
        self._op_count += 1
        op = CRDTOperation(
            file_id=file_id,
            operation_type=op_type,
            payload=payload,
            logical_timestamp=self._op_count,   # per‑peer counter
            peer_id=self.peer_id,
        )
        op.parent_hash = self._generate_hash(op)
        self.op_log.append(op)
        self._seen_hashes.add(op.parent_hash)
        self._vv[self.peer_id] = self._op_count
        return op

    # ----- helpers -----
    def _generate_hash(self, op: CRDTOperation) -> str:
        m = hashlib.sha256()
        m.update(op.file_id.encode())
        m.update(op.operation_type.name.encode())
        m.update(op.payload)
        m.update(str(int(op.logical_timestamp)).encode())
        m.update(op.peer_id.encode())
        return m.hexdigest()

    @staticmethod
    def serialize_operation(op: CRDTOperation) -> bytes:
        msg = crdt_pb2.CRDTOperation()
        msg.file_id = op.file_id
        msg.operation_type = crdt_pb2.OperationType.Value(op.operation_type.name)
        msg.payload = op.payload
        msg.logical_timestamp = int(op.logical_timestamp)
        msg.peer_id = op.peer_id
        msg.parent_hash = op.parent_hash or ""
        return msg.SerializeToString()

    @staticmethod
    def deserialize_operation(data: bytes) -> CRDTOperation:
        msg = crdt_pb2.CRDTOperation()
        msg.ParseFromString(data)
        return CRDTOperation(
            file_id=msg.file_id,
            operation_type=OperationType[msg.operation_type.name],
            payload=msg.payload,
            logical_timestamp=int(msg.logical_timestamp),
            peer_id=msg.peer_id,
            parent_hash=msg.parent_hash,
        )
