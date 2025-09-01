from __future__ import annotations
from dataclasses import dataclass
from enum import Enum, auto
from typing import List, Optional, Dict
import random
import time

HEARTBEAT_PERIOD_MS = 10_000
SUSPECT_AFTER_MS     = 3 * HEARTBEAT_PERIOD_MS
DEAD_AFTER_MS        = 5 * HEARTBEAT_PERIOD_MS
ADVERT_MAX_ENTRIES   = 20


class PeerStatus(Enum):
    ALIVE   = auto()
    SUSPECT = auto()
    DEAD    = auto()


@dataclass
class PeerInfo:
    peer_id: str
    addr: Optional[Dict] = None
    last_seen_ms: int = 0
    rtt_ms: Optional[float] = None
    status: PeerStatus = PeerStatus.SUSPECT


class PeerDirectory:
    def __init__(self) -> None:
        self._peers: Dict[str, PeerInfo] = {}

    def add_or_update(self, info: PeerInfo) -> None:
        existing = self._peers.get(info.peer_id)
        if not existing:
            self._peers[info.peer_id] = info
            return
        if info.last_seen_ms > existing.last_seen_ms:
            existing.last_seen_ms = info.last_seen_ms
        if info.addr and not existing.addr:
            existing.addr = info.addr
        if info.rtt_ms is not None:
            existing.rtt_ms = info.rtt_ms
        order = {PeerStatus.ALIVE: 3, PeerStatus.SUSPECT: 2, PeerStatus.DEAD: 1}
        if order[info.status] > order[existing.status]:
            existing.status = info.status

    def mark_heartbeat(self, peer_id: str, ts_ms: int) -> None:
        p = self._peers.get(peer_id)
        if not p:
            self._peers[peer_id] = PeerInfo(peer_id=peer_id, last_seen_ms=ts_ms, status=PeerStatus.ALIVE)
            return
        if ts_ms > p.last_seen_ms:
            p.last_seen_ms = ts_ms
        p.status = PeerStatus.ALIVE

    def sample(self, k: int, exclude: Optional[str] = None) -> List[PeerInfo]:
        candidates = [p for pid, p in self._peers.items() if p.status == PeerStatus.ALIVE and pid != exclude]
        random.shuffle(candidates)
        return candidates[:max(0, k)]

    def prune(self, now_ms: Optional[int] = None) -> None:
        if now_ms is None:
            now_ms = int(time.time() * 1000)
        for p in self._peers.values():
            age = now_ms - p.last_seen_ms
            if age >= DEAD_AFTER_MS:
                p.status = PeerStatus.DEAD
            elif age >= SUSPECT_AFTER_MS and p.status == PeerStatus.ALIVE:
                p.status = PeerStatus.SUSPECT

    def all_peers(self) -> List[PeerInfo]:
        return list(self._peers.values())

    def alive_count(self) -> int:
        return sum(1 for p in self._peers.values() if p.status == PeerStatus.ALIVE)
