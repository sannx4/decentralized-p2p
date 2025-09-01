import asyncio
import time
import json
from typing import Callable, Dict, Optional, Tuple, List

from util.storage import save_snapshot, load_snapshot
from net.session_manager import SessionManager
from net.peer_directory import PeerDirectory, PeerInfo, PeerStatus, HEARTBEAT_PERIOD_MS
from sync.gossip import GossipEngine


class DummyCRDT:
    def __init__(self, peer_id: str):
        self.peer_id = peer_id
        self.state: Dict[str, str] = {}
        self._pending: List[bytes] = []
        # try load snapshot
        snap = load_snapshot(f".data/{self.peer_id}.json")
        if isinstance(snap, dict):
            self.state.update(snap)

    def set(self, key: str, value: str) -> None:
        delta = f"set:{key}={value}".encode()
        self._apply(delta)
        self._pending.append(delta)

    def collect_deltas(self) -> List[bytes]:
        out = self._pending
        self._pending = []
        return out

    def apply_remote_delta(self, d: bytes) -> None:
        # idempotent apply + re-gossip so others also receive it
        before = dict(self.state)
        self._apply(d)
        if self.state != before:
            self._pending.append(d)

    def _apply(self, d: bytes) -> None:
        try:
            s = d.decode()
            if not s.startswith("set:"):
                return
            body = s[4:]
            k, v = body.split("=", 1)
            self.state[k] = v
        except Exception:
            pass

    # ---- Anti-entropy / persistence hooks ----
    def get_digest(self):
        items = sorted(self.state.items())
        vv = str(len(items)).encode()
        checksum = "|".join(f"{k}={v}" for k, v in items).encode()
        return vv, checksum

    def get_snapshot(self) -> bytes:
        return json.dumps(self.state, separators=(",", ":"), ensure_ascii=False).encode()

    def apply_snapshot(self, snapshot: bytes) -> None:
        try:
            data = json.loads(snapshot.decode())
            if isinstance(data, dict):
                self.state = {str(k): str(v) for k, v in data.items()}
        except Exception:
            pass

    def persist_now(self) -> None:
        save_snapshot(f".data/{self.peer_id}.json", self.state)


class FakeAgent:
    def __init__(self, name: str):
        self.name = name
        self._on_message: Optional[Callable[[bytes], None]] = None
        self._peer: Optional["FakeAgent"] = None

    def set_peer(self, other: "FakeAgent") -> None:
        self._peer = other

    def set_on_message(self, cb: Callable[[bytes], None]) -> None:
        self._on_message = cb

    async def send(self, data: bytes) -> None:
        await asyncio.sleep(0)
        if self._peer and self._peer._on_message:
            self._peer._on_message(data)

    async def close(self) -> None:
        self._peer = None


def make_pair(a_name: str, b_name: str) -> Tuple[FakeAgent, FakeAgent]:
    a = FakeAgent(a_name)
    b = FakeAgent(b_name)
    a.set_peer(b)
    b.set_peer(a)
    return a, b


async def periodic_heartbeats(ge: GossipEngine, period_ms: int):
    while True:
        await ge.send_heartbeat()
        await asyncio.sleep(period_ms / 1000.0)


A = "peer-A"
B = "peer-B"
C = "peer-C"
PEER_IDS = (A, B, C)


async def main():
    print("[demo] starting...", flush=True)

    peers = {}
    for pid in PEER_IDS:
        engine = DummyCRDT(pid)
        sessions = SessionManager(local_id=pid)
        directory = PeerDirectory()
        gossip = GossipEngine(engine=engine, sessions=sessions, directory=directory, peer_id=pid)
        peers[pid] = dict(engine=engine, sessions=sessions, directory=directory, gossip=gossip)

    ab1, ab2 = make_pair("A-to-B", "B-to-A")
    ac1, ac2 = make_pair("A-to-C", "C-to-A")

    peers[A]["sessions"].add_session(B, ab1)
    peers[B]["sessions"].add_session(A, ab2)
    peers[A]["sessions"].add_session(C, ac1)
    peers[C]["sessions"].add_session(A, ac2)

    now_ms = int(time.time() * 1000)
    peers[A]["directory"].add_or_update(PeerInfo(peer_id=B, last_seen_ms=now_ms, status=PeerStatus.ALIVE))
    peers[A]["directory"].add_or_update(PeerInfo(peer_id=C, last_seen_ms=now_ms, status=PeerStatus.ALIVE))
    peers[B]["directory"].add_or_update(PeerInfo(peer_id=A, last_seen_ms=now_ms, status=PeerStatus.ALIVE))
    peers[C]["directory"].add_or_update(PeerInfo(peer_id=A, last_seen_ms=now_ms, status=PeerStatus.ALIVE))

    for pid in PEER_IDS:
        peers[pid]["gossip"].start()
        asyncio.create_task(periodic_heartbeats(peers[pid]["gossip"], HEARTBEAT_PERIOD_MS))

    peers[A]["engine"].set("doc:title", "Hello from A")
    peers[B]["engine"].set("doc:owner", "B-user")
    peers[C]["engine"].set("doc:rev", "1")

    await asyncio.sleep(6)

    print("A state:", peers[A]["engine"].state, flush=True)
    print("B state:", peers[B]["engine"].state, flush=True)
    print("C state:", peers[C]["engine"].state, flush=True)
    print("[demo] finished", flush=True)

    # persist snapshots for all peers
    for pid in PEER_IDS:
        peers[pid]["engine"].persist_now()

    for pid in PEER_IDS:
        await peers[pid]["sessions"].close()


if __name__ == "__main__":
    asyncio.run(main())
