# sync/peer_manager.py
import asyncio, random, time
from dataclasses import dataclass, field
from typing import Dict, Optional, List, Any

@dataclass
class PeerState:
    peer_id: str
    dc: Any
    last_seen: float = field(default_factory=time.time)
    rtt_ms: Optional[float] = None
    healthy: bool = True

class PeerManager:
    def __init__(self):
        self._peers: Dict[str, PeerState] = {}
        self._lock = asyncio.Lock()

    async def add_peer(self, peer_id: str, dc: Any) -> None:
        async with self._lock:
            self._peers[peer_id] = PeerState(peer_id, dc)

    async def remove_peer(self, peer_id: str) -> None:
        async with self._lock:
            self._peers.pop(peer_id, None)

    async def mark_seen(self, peer_id: str) -> None:
        async with self._lock:
            p = self._peers.get(peer_id)
            if p:
                p.last_seen = time.time()
                p.healthy = True

    def get_dc(self, peer_id: str) -> Any:
        p = self._peers.get(peer_id)
        return p.dc if p else None

    async def random_sample(self, k: int) -> List[PeerState]:
        async with self._lock:
            peers = list(self._peers.values())
        if not peers:
            return []
        return random.sample(peers, min(k, len(peers)))

    def all(self) -> List[PeerState]:
        return list(self._peers.values())
