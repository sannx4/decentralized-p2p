# sync/engine_adapter.py
from typing import Dict, Iterable, Tuple
from p2p.proto import crdt_pb2
from crdt.types import CRDTOperation as LocalOp, OperationType as OT  # your local types

class EngineAdapter:
    def __init__(self, engine):
        self._engine = engine   # your real engine

    # GossipSync calls these:
    def clock(self) -> Dict[str, int]:
        return self._engine.version_vector()

    def ops_since(self, vv: Dict[str, int]) -> Iterable[crdt_pb2.CRDTOperation]:
        for lop in self._engine.iter_ops_since(vv):
            yield self._to_pb(lop)

    def apply_ops(self, ops_pb: Iterable[crdt_pb2.CRDTOperation]) -> Tuple[int, Dict[str,int]]:
        ops_local = [self._from_pb(op) for op in ops_pb]
        return self._engine.apply_ops(ops_local)  # or apply_ops_local if that’s your API

    # -- mapping helpers --
    @staticmethod
    def _to_pb(lop: LocalOp) -> crdt_pb2.CRDTOperation:
        return crdt_pb2.CRDTOperation(
            file_id=lop.file_id,
            operation_type=lop.operation_type.value,
            payload=lop.payload,
            logical_timestamp=int(lop.logical_timestamp),
            peer_id=lop.peer_id,
            parent_hash=getattr(lop, "parent_hash", ""),
        )

    @staticmethod
    def _from_pb(op: crdt_pb2.CRDTOperation) -> LocalOp:
        return LocalOp(
            file_id=op.file_id,
            operation_type=OT(op.operation_type),
            payload=op.payload,
            logical_timestamp=op.logical_timestamp,
            peer_id=op.peer_id,
            parent_hash=op.parent_hash,
        )
