from crdt.engine import CRDTEngine
from crdt.types import OperationType
from sync.sync_logic import get_missing_ops_since

def test_get_missing_ops_since():
    engine = CRDTEngine("localPeer")
    # Apply some ops
    engine.apply_local_operation(OperationType.CREATE, "f1", b"A")
    engine.apply_local_operation(OperationType.UPDATE, "f1", b"B")
    engine.apply_local_operation(OperationType.UPDATE, "f1", b"C")

    # Simulate remote peer with only the first operation
    missing = get_missing_ops_since(engine, peer_id="remotePeer", last_ts=1)

    assert len(missing) == 2
    assert missing[0].payload == b"B"
    assert missing[1].payload == b"C"
