# scripts/ice_two_peer_demo.py
import asyncio
from sync.bootstrap import attach_sync
from nat.ice_agent import ICEAgent     # <- adjust import if your ICEAgent is elsewhere
from p2p.proto import crdt_pb2
import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


# Minimal CRDT engine compatible with GossipSync, for demo
class DemoEngine:
    def __init__(self, local_id: str):
        self.local_id = local_id
        self._ops = []   # list[CRDTOperation]
        self._vv = {}    # site_id -> counter

    def version_vector(self):
        return dict(self._vv)

    def iter_ops_since(self, want_vv):
        for op in self._ops:
            if op.logical_timestamp > want_vv.get(op.peer_id, 0):
                yield op

    def apply_ops(self, ops):
        applied = 0
        for op in ops:
            cur = self._vv.get(op.peer_id, 0)
            if op.logical_timestamp > cur:
                self._ops.append(op)
                self._vv[op.peer_id] = op.logical_timestamp
                applied += 1
        return applied, dict(self._vv)

    def make_local_op(self, payload: bytes):
        counter = self._vv.get(self.local_id, 0) + 1
        op = crdt_pb2.CRDTOperation(
            file_id="doc",
            operation_type=crdt_pb2.UPDATE,
            payload=payload,
            logical_timestamp=counter,
            peer_id=self.local_id,
            parent_hash="",
        )
        self.apply_ops([op])

async def main():
    # Two peers in one process using two aioice connections
    id1, id2 = "A", "B"
    eng1, eng2 = DemoEngine(id1), DemoEngine(id2)
    agent1, agent2 = ICEAgent(name=id1), ICEAgent(name=id2)

    # Attach gossip to both agents
    rt1 = attach_sync(local_id=id1, engine=eng1, ice_agent=agent1, debug=True, round_interval=0.3, fanout=1)
    rt2 = attach_sync(local_id=id2, engine=eng2, ice_agent=agent2, debug=True, round_interval=0.3, fanout=1)
    agent1.set_runtime(rt1)
    agent2.set_runtime(rt2)

    # Gather ICE on both sides
    c1, u1, p1 = await agent1.gather_candidates()
    c2, u2, p2 = await agent2.gather_candidates()

    # Cross-connect
    await asyncio.gather(
        agent1.start_connection(c2, u2, p2),
        agent2.start_connection(c1, u1, p1),
    )

    # Bind gossip over the ICE pipes
    await asyncio.gather(
        agent1.bind_gossip(id1),
        agent2.bind_gossip(id2),
    )

    # Make a couple of ops on A, let gossip spread them
    eng1.make_local_op(b"a1")
    eng1.make_local_op(b"a2")
    await asyncio.sleep(2.0)

    print("VV1:", eng1.version_vector(), "VV2:", eng2.version_vector())
    assert eng1.version_vector() == eng2.version_vector(), "Peers did not converge"
    print("Converged ✅")

if __name__ == "__main__":
    asyncio.run(main())
