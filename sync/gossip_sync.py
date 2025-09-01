# sync/gossip_sync.py
import asyncio
import uuid
from typing import Dict, Callable, Optional
from p2p.proto import sync_pb2


class GossipSync:
    def __init__(
        self,
        local_id: str,
        engine,                               # EngineAdapter
        peer_manager,                         # PeerManager
        send_fn: Callable[[str, bytes], None],
        round_interval: float = 1.5,
        fanout: int = 3,
        max_bytes: int = 64 * 1024,
        max_ops: int = 2048,
        room_id: Optional[str] = None,        # 🔸 ADDED: scope messages to a room/doc
    ):
        self.local_id = local_id
        self.engine = engine
        self.pm = peer_manager
        self.send_fn = send_fn
        self.round_interval = round_interval
        self.fanout = fanout
        self.max_bytes = max_bytes
        self.max_ops = max_ops
        self.room_id = room_id               # 🔸 ADDED
        self._task: Optional[asyncio.Task] = None

    async def start(self):
        if not self._task:
            self._task = asyncio.create_task(self._gossip_loop())

    async def stop(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _gossip_loop(self):
        while True:
            peers = await self.pm.random_sample(self.fanout)
            if peers:
                vv_pb = self._vv_to_proto(self.engine.clock())
                digest = sync_pb2.Digest(vv=vv_pb)
                for p in peers:
                    self._send_envelope(p.peer_id, digest=digest)
            await asyncio.sleep(self.round_interval)

    async def on_message(self, peer_id: str, data: bytes):
        env = sync_pb2.Envelope()
        env.ParseFromString(data)

        # 🔸 ADDED: ignore messages for a different room/doc (if both sides set room)
        if self.room_id and env.room and env.room != self.room_id:
            return

        which = env.WhichOneof("payload")
        if which == "digest":
            await self._handle_digest(peer_id, env.digest)
        elif which == "delta_req":
            await self._handle_delta_req(peer_id, env.delta_req)
        elif which == "delta_chunk":
            await self._handle_delta_chunk(peer_id, env.delta_chunk)
        # Ack is optional/telemetry; ignore for now.

    async def _handle_digest(self, peer_id: str, remote_digest: sync_pb2.Digest):
        local_vv = self.engine.clock()
        remote_vv = self._vv_from_proto(remote_digest.vv)

        # If they’re ahead anywhere → request deltas
        ahead = any(remote_vv.get(s, 0) > local_vv.get(s, 0) for s in remote_vv)
        if ahead:
            want = {s: local_vv.get(s, 0) for s in set(local_vv) | set(remote_vv)}
            req = sync_pb2.DeltaRequest(
                want_from=self._vv_to_proto(want),
                max_bytes=self.max_bytes,
                max_ops=self.max_ops,
            )
            self._send_envelope(peer_id, delta_req=req)

        # If we’re ahead anywhere → proactively push deltas
        we_ahead = any(local_vv.get(s, 0) > remote_vv.get(s, 0) for s in local_vv)
        if we_ahead:
            want = {s: remote_vv.get(s, 0) for s in set(local_vv) | set(remote_vv)}
            await self._serve_delta(peer_id, want)

    async def _handle_delta_req(self, peer_id: str, req: sync_pb2.DeltaRequest):
        want = self._vv_from_proto(req.want_from)
        await self._serve_delta(
            peer_id,
            want,
            req.max_bytes or self.max_bytes,
            req.max_ops or self.max_ops,
        )

    async def _serve_delta(
        self,
        peer_id: str,
        want: Dict[str, int],
        max_bytes: Optional[int] = None,
        max_ops: Optional[int] = None,
    ):
        max_bytes = max_bytes or self.max_bytes
        max_ops = max_ops or self.max_ops

        buf_ops = []
        size = 0
        for op in self.engine.ops_since(want):
            bs = op.SerializeToString()
            # Flush if adding this op would exceed limits
            if buf_ops and (size + len(bs) > max_bytes or len(buf_ops) >= max_ops):
                self._send_envelope(peer_id, delta_chunk=self._chunk(buf_ops, last=False))
                buf_ops, size = [], 0
            buf_ops.append(op)
            size += len(bs)

        # Final chunk (even if empty, signals completeness)
        self._send_envelope(peer_id, delta_chunk=self._chunk(buf_ops, last=True))

    async def _handle_delta_chunk(self, peer_id: str, chunk: sync_pb2.DeltaChunk):
        # Apply operations into the engine
        applied, _ = self.engine.apply_ops(chunk.ops)
        # Optionally: mark seen / telemetry here

    # ---------- helpers ----------
    def _send_envelope(self, peer_id: str, digest=None, delta_req=None, delta_chunk=None):
        # `from` is a reserved keyword in Python; pass via kwargs dict
        env = sync_pb2.Envelope(msg_id=str(uuid.uuid4()), **{"from": self.local_id})
        # 🔸 ADDED: set room tag if provided
        if self.room_id:
            env.room = self.room_id
        if digest is not None:
            env.digest.CopyFrom(digest)
        if delta_req is not None:
            env.delta_req.CopyFrom(delta_req)
        if delta_chunk is not None:
            env.delta_chunk.CopyFrom(delta_chunk)
        self.send_fn(peer_id, env.SerializeToString())

    @staticmethod
    def _vv_to_proto(vv: Dict[str, int]):
        pb = sync_pb2.VersionVector()
        for s, c in vv.items():
            pb.entries.append(sync_pb2.VersionVectorEntry(site_id=s, counter=c))
        return pb

    @staticmethod
    def _vv_from_proto(pb: sync_pb2.VersionVector) -> Dict[str, int]:
        return {e.site_id: e.counter for e in pb.entries}

    @staticmethod
    def _chunk(ops, last: bool):
        ch = sync_pb2.DeltaChunk()
        ch.ops.extend(ops)
        ch.last = last
        return ch
