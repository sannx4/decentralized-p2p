import asyncio
import time
from typing import Optional

from sync.dedupe import DedupeTable
from net.session_manager import SessionManager
from net.peer_directory import PeerDirectory
from proto import multi_pb2
from sync.anti_entropy import AntiEntropy
from util.metrics import incr
from util.log import log

GOSSIP_INTERVAL_MS = 2000
FANOUT = 3


class GossipEngine:
    def __init__(self, engine, sessions: SessionManager, directory: PeerDirectory, peer_id: str):
        self.engine = engine
        self.sessions = sessions
        self.directory = directory
        self.peer_id = peer_id

        self._seq_no = 0
        self._running = False
        self._task: Optional[asyncio.Task] = None

        self._dedupe = DedupeTable()
        self.sessions.set_on_message(self._on_message)
        self._ae = AntiEntropy(engine, sessions, directory, peer_id)

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._gossip_loop())
        self._ae.start()
        log("gossip_start", peer_id=self.peer_id)

    def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            self._task = None
        self._ae.stop()
        log("gossip_stop", peer_id=self.peer_id)

    async def _gossip_loop(self) -> None:
        try:
            while self._running:
                await asyncio.sleep(GOSSIP_INTERVAL_MS / 1000.0)
                await self._gossip_round()
        except asyncio.CancelledError:
            return

    async def _gossip_round(self) -> None:
        peers = self.directory.sample(FANOUT, exclude=self.peer_id)
        incr("gossip_rounds", 1)
        if not peers:
            return
        deltas = getattr(self.engine, "collect_deltas", lambda: [])()
        if not deltas:
            return
        bundle = multi_pb2.DeltaBundle()
        bundle.deltas.extend(deltas)
        env = self._wrap(multi_pb2.Envelope.DELTA, bundle.SerializeToString())
        payload = env.SerializeToString()
        for p in peers:
            await self.sessions.send(p.peer_id, payload)
            incr("deltas_sent", len(deltas))
        log("gossip_push", from_id=self.peer_id, fanout=len(peers), deltas=len(deltas))

    def _wrap(self, typ: int, payload: bytes) -> multi_pb2.Envelope:
        self._seq_no += 1
        env = multi_pb2.Envelope()
        env.from_id = self.peer_id
        env.seq_no = self._seq_no
        env.type = typ
        env.payload = payload
        # --- AUTH: attach token if configured ---
        if getattr(self.sessions, "cluster_token", None):
            env.token = self.sessions.cluster_token  # type: ignore[attr-defined]
        return env

    def _on_message(self, from_id: str, raw: bytes) -> None:
        try:
            env = multi_pb2.Envelope()
            env.ParseFromString(raw)
        except Exception:
            incr("envelope_parse_errors", 1)
            return

        # --- AUTH: drop if token mismatch when token is configured ---
        want = getattr(self.sessions, "cluster_token", None)
        if want is not None:
            if (env.token or "") != want:
                incr("auth_dropped", 1)
                log("auth_drop", to_id=self.peer_id, from_id=env.from_id)
                return

        if self._dedupe.already_seen(env.from_id, int(env.seq_no)):
            incr("duplicates_dropped", 1)
            return

        if env.type == multi_pb2.Envelope.HB:
            try:
                hb = multi_pb2.Heartbeat()
                hb.ParseFromString(env.payload)
                self.directory.mark_heartbeat(env.from_id, hb.ts_ms)
                incr("hb_recv", 1)
            except Exception:
                incr("hb_errors", 1)

        elif env.type == multi_pb2.Envelope.DELTA:
            try:
                bundle = multi_pb2.DeltaBundle()
                bundle.ParseFromString(env.payload)
                changed = False
                for d in bundle.deltas:
                    before = getattr(self.engine, "state", None)
                    self.engine.apply_remote_delta(d)
                    changed = True
                if changed:
                    asyncio.create_task(self._gossip_round())
                incr("deltas_recv", len(bundle.deltas))
                log("gossip_recv_delta", to_id=self.peer_id, from_id=env.from_id, deltas=len(bundle.deltas))
            except Exception:
                incr("delta_errors", 1)

        elif env.type == multi_pb2.Envelope.DIGEST:
            try:
                dg = multi_pb2.Digest()
                dg.ParseFromString(env.payload)
                asyncio.create_task(self._ae.on_digest(env.from_id, dg))
                incr("digests_recv", 1)
            except Exception:
                incr("digest_errors", 1)

        elif env.type == multi_pb2.Envelope.SNAPSHOT_RESP:
            try:
                sr = multi_pb2.SnapshotResponse()
                sr.ParseFromString(env.payload)
                asyncio.create_task(self._ae.on_snapshot_resp(sr))
                incr("snapshots_recv", 1)
            except Exception:
                incr("snapshot_errors", 1)

    async def send_heartbeat(self) -> None:
        hb = multi_pb2.Heartbeat()
        hb.from_id = self.peer_id
        hb.ts_ms = int(time.time() * 1000)
        env = self._wrap(multi_pb2.Envelope.HB, hb.SerializeToString())
        payload = env.SerializeToString()
        peers = self.directory.sample(FANOUT, exclude=self.peer_id)
        for p in peers:
            await self.sessions.send(p.peer_id, payload)
            incr("hb_sent", 1)
        log("heartbeat_push", from_id=self.peer_id, fanout=len(peers))
