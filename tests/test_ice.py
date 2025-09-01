# tests/test_ice.py
import asyncio
import pytest
from nat.ice_agent import ICEAgent
from crdt.types import CRDTOperation, OperationType

@pytest.mark.asyncio
async def test_ice_peer_to_peer_connection():
    agent_a = ICEAgent("PeerA")
    agent_b = ICEAgent("PeerB")

    # Step 1: Both gather their local candidates
    candidates_a, ufrag_a, pwd_a = await agent_a.gather_candidates()
    candidates_b, ufrag_b, pwd_b = await agent_b.gather_candidates()

    # Step 2: Exchange ICE credentials and candidates
    await asyncio.gather(
        agent_a.start_connection(candidates_b, ufrag_b, pwd_b),
        agent_b.start_connection(candidates_a, ufrag_a, pwd_a)
    )

    # Step 3: A sends CRDT operation → B
    dummy_op_a = CRDTOperation(
        file_id="testA",
        operation_type=OperationType.CREATE,
        payload=b"Hello from A!",
        logical_timestamp=1,
        peer_id="PeerA"
    )
    await agent_a.send_crdt_operation(dummy_op_a)
    received_op_b = await agent_b.receive_crdt_operation()
    assert received_op_b.payload == b"Hello from A!"
    assert received_op_b.peer_id == "PeerA"

    # Step 4: B sends CRDT operation → A
    dummy_op_b = CRDTOperation(
        file_id="testB",
        operation_type=OperationType.CREATE,
        payload=b"Hi from B!",
        logical_timestamp=1,
        peer_id="PeerB"
    )
    await agent_b.send_crdt_operation(dummy_op_b)
    received_op_a = await agent_a.receive_crdt_operation()
    assert received_op_a.payload == b"Hi from B!"
    assert received_op_a.peer_id == "PeerB"

    await asyncio.gather(agent_a.close(), agent_b.close())
