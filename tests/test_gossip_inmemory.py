# tests/test_gossip_inmemory.py
import asyncio
import pytest

from p2p.proto import crdt_pb2, sync_pb2
from sync.engine_adapter import EngineAdapter
from sync.peer_manager import PeerManager
from sync.gossip_sync import GossipSync

pytestmark = pytest.mark.asyncio


# ----------------------------
# Fake CRDT Engine (minimal)
# ----------------------------
class FakeEngine:
    """
    Minimal engine implementing the interface used by EngineAdapter:
      - version_vector() -> Dict[str,int]
      - iter_ops_since(vv) -> Iterable[CRDTOperation]
      - apply_ops(ops) -> (applied_count, new_vv)

    We treat:
      peer_id            = site_id
      logical_timestamp  = per-site counter (monotonic per peer in this test)
    """
    def __init__(self, local_id: str):
        self.local_id = local_id
        self._ops = []   # list[CRDTOperation]
        self._vv = {}    # site_id -> max logical_timestamp seen

    # For testing: create a local op
    def make_local_op(self, file_id="f1", op_type=crdt_pb2.UPDATE, payload=b"x"):
        counter = self._vv.get(self.local_id, 0) + 1
        op = crdt_pb2.CRDTOperation(
            file_id=file_id,
            operation_type=op_type,
            payload=payload,
            logical_timestamp=counter,   # use as per-site counter
            peer_id=self.local_id,
            parent_hash="",
        )
        self.apply_ops([op])  # applying also appends locally
        return op

    # ---- interface used by EngineAdapter ----
    def version_vector(self):
        return dict(self._vv)

    def iter_ops_since(self, want_vv):
        for op in self._ops:
            seen = want_vv.get(op.peer_id, 0)
            if op.logical_timestamp > seen:
                yield op

    def apply_ops(self, ops):
        applied = 0
        for op in ops:
            cur = self._vv.get(op.peer_id, 0)
            if op.logical_timestamp > cur:  # accept strictly new ops
                self._ops.append(op)
                self._vv[op.peer_id] = op.logical_timestamp
                applied += 1
            # else: already seen -> ignore (idempotent)
        return applied, dict(self._vv)


# ----------------------------
# In-memory network (fixed)
# ----------------------------
class InMemoryNetwork:
    """
    Tiny router that delivers bytes and includes the sender id.
    """
    def __init__(self):
        # peer_id -> handler(from_peer: str, data: bytes)
        self.endpoints = {}

    def register(self, peer_id: str, handler):
        self.endpoints[peer_id] = handler

    def send(self, from_peer: str, to_peer: str, data: bytes):
        handler = self.endpoints[to_peer]
        loop = asyncio.get_event_loop()
        # Schedule as task to simulate async delivery
        loop.create_task(handler(from_peer, data))


# ----------------------------
# Node wrapper
# ----------------------------
class Node:
    def __init__(self, peer_id: str, net: InMemoryNetwork):
        self.id = peer_id
        self.engine = FakeEngine(peer_id)
        self.adapter = EngineAdapter(self.engine)
        self.pm = PeerManager()
        self.net = net

        # send_fn used by GossipSync (include our id as sender)
        def send_fn(peer_id: str, payload: bytes):
            self.net.send(self.id, peer_id, payload)

        self.gossip = GossipSync(
            local_id=self.id,
            engine=self.adapter,
            peer_manager=self.pm,
            send_fn=send_fn,
            round_interval=0.2,   # faster rounds for test
            fanout=2,
            max_bytes=64 * 1024,
            max_ops=1000,
        )

        # Register network receive handler: now accepts from_peer
        async def on_net_msg(from_peer: str, data: bytes):
            await self.gossip.on_message(from_peer, data)

        self.net.register(self.id, on_net_msg)

        # Dummy DC for PeerManager (not used here but required by interface)
        class DummyDC:
            def send(self, _): pass
        self.dc = DummyDC()

    async def start(self):
        await self.gossip.start()

    async def add_peer(self, other_id: str):
        await self.pm.add_peer(other_id, self.dc)

    def vv(self):
        return self.engine.version_vector()


# ----------------------------
# The actual test
# ----------------------------
async def test_three_peer_convergence_in_memory():
    net = InMemoryNetwork()
    a = Node("A", net)
    b = Node("B", net)
    c = Node("C", net)

    # Each node knows about the others (PeerManager entries)
    await a.add_peer("B"); await a.add_peer("C")
    await b.add_peer("A"); await b.add_peer("C")
    await c.add_peer("A"); await c.add_peer("B")

    # Start gossip loops
    await a.start(); await b.start(); await c.start()

    # Make some ops on A and B
    a.engine.make_local_op(file_id="doc", payload=b"a1")
    a.engine.make_local_op(file_id="doc", payload=b"a2")
    b.engine.make_local_op(file_id="doc", payload=b"b1")

    # Let gossip run for a bit (bump to 2s to be safe)
    await asyncio.sleep(2.0)

    # All should converge: same version vectors
    vv_a, vv_b, vv_c = a.vv(), b.vv(), c.vv()
    assert vv_a == vv_b == vv_c, f"Not converged: {vv_a=} {vv_b=} {vv_c=}"
