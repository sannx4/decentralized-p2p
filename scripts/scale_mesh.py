import asyncio
import time
from typing import Callable, Dict, Optional, Tuple, List
import random

from net.session_manager import SessionManager
from net.peer_directory import PeerDirectory, PeerInfo, PeerStatus, HEARTBEAT_PERIOD_MS
from sync.gossip import GossipEngine
# make gossip faster/stronger for the test
import sync.gossip as gossip
gossip.FANOUT = 12           # hub can hit everyone each round
gossip.GOSSIP_INTERVAL_MS = 500  # 2 rounds/sec



class DummyCRDT:
    def __init__(self, peer_id: str):
        self.peer_id = peer_id
        self.state: Dict[str, str] = {}
        self._pending: List[bytes] = []

    def set(self, key: str, value: str) -> None:
        delta = f"set:{key}={value}".encode()
        self._apply(delta)
        self._pending.append(delta)

    def collect_deltas(self) -> List[bytes]:
        out = self._pending
        self._pending = []
        return out

    def apply_remote_delta(self, d: bytes) -> None:
        # re-gossip remote deltas so they propagate further
        before = dict(self.state)
        self._apply(d)
        if self.state != before:
            self._pending.append(d)

    def _apply(self, d: bytes) -> None:
        try:
            s = d.decode()
            if s.startswith("set:"):
                k, v = s[4:].split("=", 1)
                self.state[k] = v
        except Exception:
            pass


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


async def main(n_peers: int = 12):
    peer_ids = [f"peer-{i}" for i in range(n_peers)]
    hub = peer_ids[0]

    peers = {}
    for pid in peer_ids:
        engine = DummyCRDT(pid)
        sessions = SessionManager(local_id=pid, cluster_token="demo-token")
        directory = PeerDirectory()
        gossip = GossipEngine(engine=engine, sessions=sessions, directory=directory, peer_id=pid)
        peers[pid] = dict(engine=engine, sessions=sessions, directory=directory, gossip=gossip)

    # wire a star topology via hub
    links = []
    for pid in peer_ids[1:]:
        a, b = make_pair(f"{hub}-to-{pid}", f"{pid}-to-{hub}")
        peers[hub]["sessions"].add_session(pid, a)
        peers[pid]["sessions"].add_session(hub, b)
        now_ms = int(time.time() * 1000)
        peers[hub]["directory"].add_or_update(PeerInfo(peer_id=pid, last_seen_ms=now_ms, status=PeerStatus.ALIVE))
        peers[pid]["directory"].add_or_update(PeerInfo(peer_id=hub, last_seen_ms=now_ms, status=PeerStatus.ALIVE))
        links.append((a, b))

    for pid in peer_ids:
        peers[pid]["gossip"].start()
        asyncio.create_task(periodic_heartbeats(peers[pid]["gossip"], HEARTBEAT_PERIOD_MS))

    # inject writes at random peers
    for k in range(5):
        tgt = random.choice(peer_ids)
        peers[tgt]["engine"].set(f"k{k}", f"v{tgt}")

    await asyncio.sleep(12)  # give enough rounds to converge

    ok = True
    for pid in peer_ids:
        state = peers[pid]["engine"].state
        if not all(f"k{i}" in state for i in range(5)):
            ok = False
        print(pid, len(state), state)

    for pid in peer_ids:
        await peers[pid]["sessions"].close()

    print("converged:", ok)


if __name__ == "__main__":
    asyncio.run(main())
