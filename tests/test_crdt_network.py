import pytest
import asyncio
from nat.ice_agent import ICEAgent
from crdt.engine import CRDTEngine, OperationType
from p2p.proto import crdt_pb2

@pytest.mark.asyncio
async def test_crdt_op_transfer_between_peers():
    engine_a = CRDTEngine("peerA")
    engine_b = CRDTEngine("peerB")
    agent_a = ICEAgent("peerA")
    agent_b = ICEAgent("peerB")

    # Setup connection
    cand_a, ufrag_a, pwd_a = await agent_a.gather_candidates()
    cand_b, ufrag_b, pwd_b = await agent_b.gather_candidates()
    await asyncio.gather(
        agent_a.start_connection(cand_b, ufrag_b, pwd_b),
        agent_b.start_connection(cand_a, ufrag_a, pwd_a)
    )

    # A creates an op
    op = engine_a.apply_local_operation(OperationType.CREATE, "file123", b"Hello Network!")

    # Send from A to B
    await agent_a.send_crdt_operation(op)

    # Receive at B
    received_msg = await agent_b.receive_crdt_operation()
    assert received_msg.file_id == "file123"
