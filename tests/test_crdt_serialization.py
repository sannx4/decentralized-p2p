from proto import crdt_pb2

def test_crdt_operation_serialization():
    original = crdt_pb2.CRDTOperation(
        file_id="file42",
        operation_type=crdt_pb2.OperationType.CREATE,
        payload=b"Hello",
        logical_timestamp=1,
        peer_id="peerA",
        parent_hash="abc123"
    )
    serialized = original.SerializeToString()

    decoded = crdt_pb2.CRDTOperation()  # ✅ Fixed line
    decoded.ParseFromString(serialized)

    assert original == decoded

