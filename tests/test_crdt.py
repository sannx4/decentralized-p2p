from crdt.engine import CRDTEngine
from crdt.types import OperationType


def test_crdt_merge_conflict_free():
    engine_a = CRDTEngine("peerA")
    engine_b = CRDTEngine("peerB")

    op1 = engine_a.apply_local_operation(OperationType.CREATE, "file1", b"Hello")
    op2 = engine_b.apply_local_operation(OperationType.UPDATE, "file1", b"World")

    engine_a.merge_remote_operations([op2])
    engine_b.merge_remote_operations([op1])

    assert engine_a.state_vector["file1"] == engine_b.state_vector["file1"]
    assert len(engine_a.op_log) == len(engine_b.op_log) == 2
