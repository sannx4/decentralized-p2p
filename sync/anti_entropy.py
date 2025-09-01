import asyncio
import random
import time
from typing import Optional, List, Tuple

from net.session_manager import SessionManager
from net.peer_directory import PeerDirectory
from proto import multi_pb2

AE_INTERVAL_MS = 60_000  # jittered around this


class AntiEntropy:
    """
    Minimal anti-entropy helper.
    Expects the CRDT engine to optionally implement:
      - get_digest() -> tuple[bytes, bytes]  (vv, checksum)     [optional]
      - get_snapshot() -> bytes                                 [optional]
      - delta_since(vv: bytes) -> list[bytes]                   [optional]
      - apply_snapshot(snapshot: bytes) -> None                 [optional]
    If methods are missing, it safely no-ops.
    """
    def __init__(self, engine, sessions: SessionManager, directory: PeerDirectory, peer_id: str):
        self.engine = engine
        self.sessions = sessions
        self.directory = directory
        self.peer_id = peer_id
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._seq_no = 0  # local seq just for envelopes we send

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop())

    def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            self._task = None

    async def _loop(self) -> None:
        try:
            while self._running:
                delay = AE_INTERVAL_MS + random.randint(-10_000, 10_000)
                await asyncio.sleep(max(5_000, delay) / 1000.0)
                await self._probe_one_peer()
        except asyncio.CancelledError:
            return

    async def _probe_one_peer(self) -> None:
        peers = self.directory.sample(1, exclude=self.peer_id)
        if not peers:
            return
        p = peers[0]
        # send our digest if engine supports it
        if hasattr(self.engine, "get_digest"):
            vv, checksum = self._safe_get_digest()
            dg = multi_pb2.Digest(vv=vv, checksum=checksum)
            env = self._wrap(multi_pb2.Envelope.DIGEST, dg.SerializeToString())
            await self.sessions.send(p.peer_id, env.SerializeToString())

    def _wrap(self, typ: int, payload: bytes) -> multi_pb2.Envelope:
        self._seq_no += 1
        env = multi_pb2.Envelope()
        env.from_id = self.peer_id
        env.seq_no = self._seq_no
        env.type = typ
        env.payload = payload
        return env

    # ---- Handlers called by gossip router ----
    async def on_digest(self, from_id: str, dg: multi_pb2.Digest) -> None:
        # Try to compute what the sender might be missing; if we can't, fallback to snapshot (if available).
        if hasattr(self.engine, "delta_since"):
            try:
                missing = self.engine.delta_since(bytes(dg.vv))
            except Exception:
                missing = None
        else:
            missing = None

        if missing:
            bundle = multi_pb2.DeltaBundle()
            bundle.deltas.extend(missing)
            env = self._wrap(multi_pb2.Envelope.PULL_RESP, bundle.SerializeToString())
            await self.sessions.send(from_id, env.SerializeToString())
            return

        # If no delta path, try to supply a snapshot when possible.
        if hasattr(self.engine, "get_snapshot"):
            snap_bytes = self._safe_get_snapshot()
            if snap_bytes is not None:
                env = self._wrap(multi_pb2.Envelope.SNAPSHOT_RESP,
                                 multi_pb2.SnapshotResponse(snapshot=snap_bytes).SerializeToString())
                await self.sessions.send(from_id, env.SerializeToString())

    async def on_snapshot_resp(self, snap: multi_pb2.SnapshotResponse) -> None:
        if hasattr(self.engine, "apply_snapshot"):
            try:
                self.engine.apply_snapshot(bytes(snap.snapshot))
            except Exception:
                pass

    # ---- Safe wrappers ----
    def _safe_get_digest(self) -> Tuple[bytes, bytes]:
        try:
            vv, checksum = self.engine.get_digest()  # type: ignore[attr-defined]
            vv = vv or b""
            checksum = checksum or b""
            return vv, checksum
        except Exception:
            return b"", b""

    def _safe_get_snapshot(self) -> Optional[bytes]:
        try:
            snap = self.engine.get_snapshot()  # type: ignore[attr-defined]
            return bytes(snap) if snap is not None else None
        except Exception:
            return None
