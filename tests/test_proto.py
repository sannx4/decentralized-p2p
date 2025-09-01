from p2p.proto import messages_pb2

def test_discovery_message_serialization():
    msg = messages_pb2.DiscoveryMessage(
        peer_id="peer-001",
        peer_name="Alice",
        capabilities=["sync", "relay"],
        timestamp=1234567890
    )
    serialized = msg.SerializeToString()
    deserialized = messages_pb2.DiscoveryMessage.FromString(serialized)
    assert deserialized.peer_id == "peer-001"
        # etc.
