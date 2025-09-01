# tests/test_sync_protocol.py

import asyncio
import pytest

from nat.ice_agent import ICEAgent
from crdt.engine import CRDTEngine
from crdt.types import OperationType
from sync.sync_logic import sync_from_peer

@pytest.mark.asyncio
async def test_bidirectional_crdt_sync():
    # Step 1: Setup two engines and ICE agents
    engine_a = CRDTEngine("PeerA")
    engine_b = CRDTEngine("PeerB")
    agent_a = ICEAgent("PeerA")
    agent_b = ICEAgent("PeerB")

    # Step 2: Gather ICE candidates and establish connection
    cand_a, ufrag_a, pwd_a = await agent_a.gather_candidates()
    cand_b, ufrag_b, pwd_b = await agent_b.gather_candidates()

    await asyncio.gather(
        agent_a.start_connection(cand_b, ufrag_b, pwd_b),
        agent_b.start_connection(cand_a, ufrag_a, pwd_a)
    )

    # Step 3: Apply local ops to both engines independently
    engine_a.apply_local_operation(OperationType.CREATE, "file1", b"A")
    engine_a.apply_local_operation(OperationType.UPDATE, "file1", b"B")

    engine_b.apply_local_operation(OperationType.CREATE, "file2", b"X")
    engine_b.apply_local_operation(OperationType.UPDATE, "file2", b"Y")

    # Step 4: Start bidirectional sync
    await asyncio.gather(
        sync_from_peer(engine_a, agent_a),
        sync_from_peer(engine_b, agent_b),
    )

    # Step 5: Validate that both peers have all 4 operations
    assert len(engine_a.op_log) == 4
    assert len(engine_b.op_log) == 4

    # Step 6: Check state vectors match for full sync
    assert engine_a.state_vector == engine_b.state_vector

    await asyncio.gather(agent_a.close(), agent_b.close())
