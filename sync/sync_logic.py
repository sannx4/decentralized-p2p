from nat.ice_agent import ICEAgent
from typing import Dict
import asyncio
from crdt.engine import CRDTEngine
from crdt.types import CRDTOperation

async def sync_from_peer(local_engine: CRDTEngine, remote_agent: ICEAgent):
    """
    Two-way CRDT sync:
    1. Exchange state vectors.
    2. Send missing operations.
    3. Receive and apply remote operations.
    """
    # Step 1: Send local state vector
    await remote_agent.send_data(local_engine.state_vector)

    # Step 2: Receive remote state vector
    remote_state: Dict[str, int] = await remote_agent.receive_data()

    # Step 3: Send missing ops to remote
    for file_id, local_ts in local_engine.state_vector.items():
        remote_ts = remote_state.get(file_id, 0)
        for op in local_engine.op_log:
            if op.file_id == file_id and op.logical_timestamp > remote_ts:
                await remote_agent.send_crdt_operation(op)

    # Optional: delay before receiving remote ops
    await asyncio.sleep(0.2)

    # Step 4: Receive remote missing ops and apply
    while True:
        try:
            op = await asyncio.wait_for(remote_agent.receive_crdt_operation(), timeout=1)
            local_engine.merge_remote_operations([op])
        except asyncio.TimeoutError:
            break
