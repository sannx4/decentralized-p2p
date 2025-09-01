import asyncio
from typing import Dict, Optional, List, Callable

from nat.ice_agent import ICEAgent  # your existing transport
from util.metrics import incr
from util.log import log

MAX_QUEUE_BYTES_PER_PEER = 512 * 1024
MAX_QUEUE_ITEMS_PER_PEER = 1024
DRAIN_BATCH_BYTES        = 64 * 1024


class SessionManager:
    def __init__(self, local_id: str, cluster_token: Optional[str] = None) -> None:
        self.local_id = local_id
        self.cluster_token = cluster_token
        self._sessions: Dict[str, ICEAgent] = {}
        self._send_queues: Dict[str, asyncio.Queue[bytes]] = {}
        self._queue_bytes: Dict[str, int] = {}
        self._drainers: Dict[str, asyncio.Task] = {}
        self._on_message: Optional[Callable[[str, bytes], None]] = None

    def set_on_message(self, handler: Callable[[str, bytes], None]) -> None:
        self._on_message = handler

    def add_session(self, peer_id: str, agent: ICEAgent) -> None:
        """Register an already-open ICEAgent under this SessionManager."""
        if peer_id in self._sessions:
            return
        self._sessions[peer_id] = agent
        q: asyncio.Queue[bytes] = asyncio.Queue()
        self._send_queues[peer_id] = q
        self._queue_bytes[peer_id] = 0

        if hasattr(agent, "set_on_message"):
            agent.set_on_message(lambda data: self._deliver(peer_id, data))  # type: ignore

        self._drainers[peer_id] = asyncio.create_task(self._drain_loop(peer_id))
        incr("session_connects", 1)
        log("session_open", local_id=self.local_id, peer_id=peer_id)

    async def connect(self, peer_id: str, addr: str) -> None:
        # optional: create a new ICEAgent yourself here
        return

    async def send(self, peer_id: str, data: bytes) -> None:
        q = self._send_queues.get(peer_id)
        if q is None:
            log("send_drop_no_session", local_id=self.local_id, peer_id=peer_id, bytes=len(data))
            incr("send_drops_no_session", 1)
            return
        # backpressure by items
        if q.qsize() >= MAX_QUEUE_ITEMS_PER_PEER:
            try:
                oldest = q.get_nowait()
                self._queue_bytes[peer_id] = max(0, self._queue_bytes[peer_id] - len(oldest))
                incr("items_dropped_backpressure", 1)
                log("backpressure_drop_item", local_id=self.local_id, peer_id=peer_id, queue_len=q.qsize())
            except asyncio.QueueEmpty:
                pass
        # backpressure by bytes
        cur = self._queue_bytes.get(peer_id, 0)
        if cur + len(data) > MAX_QUEUE_BYTES_PER_PEER:
            while cur + len(data) > MAX_QUEUE_BYTES_PER_PEER and not q.empty():
                oldest = await q.get()
                cur -= len(oldest)
                incr("bytes_dropped_backpressure", len(oldest))
            self._queue_bytes[peer_id] = max(0, cur)
            if cur + len(data) > MAX_QUEUE_BYTES_PER_PEER:
                incr("items_dropped_backpressure", 1)
                log("backpressure_drop_too_big", local_id=self.local_id, peer_id=peer_id, msg_bytes=len(data))
                return

        await q.put(data)
        self._queue_bytes[peer_id] = self._queue_bytes.get(peer_id, 0) + len(data)
        incr("bytes_enqueued", len(data))

    async def broadcast(self, peer_ids: List[str], data: bytes) -> None:
        for pid in peer_ids:
            await self.send(pid, data)

    async def _drain_loop(self, peer_id: str) -> None:
        q = self._send_queues[peer_id]
        agent = self._sessions[peer_id]
        try:
            while True:
                chunk = bytearray()
                item = await q.get()
                chunk += item
                self._queue_bytes[peer_id] = max(0, self._queue_bytes[peer_id] - len(item))
                while not q.empty() and len(chunk) < DRAIN_BATCH_BYTES:
                    item = await q.get()
                    chunk += item
                    self._queue_bytes[peer_id] = max(0, self._queue_bytes[peer_id] - len(item))
                if hasattr(agent, "send"):
                    await agent.send(bytes(chunk))  # type: ignore
                    incr("bytes_sent", len(chunk))
        except asyncio.CancelledError:
            return
        except Exception as e:
            incr("drain_errors", 1)
            log("drain_error", local_id=self.local_id, peer_id=peer_id, error=str(e))
            await asyncio.sleep(0.1)

    def _deliver(self, peer_id: str, data: bytes) -> None:
        incr("bytes_received", len(data))
        if self._on_message:
            try:
                self._on_message(peer_id, data)
            except Exception as e:
                incr("deliver_errors", 1)
                log("deliver_error", local_id=self.local_id, peer_id=peer_id, error=str(e))

    def active_peers(self) -> List[str]:
        return list(self._sessions.keys())

    async def close_peer(self, peer_id: str) -> None:
        """Close and remove a single peer session cleanly."""
        # stop the drain task
        task = self._drainers.pop(peer_id, None)
        if task:
            try:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            except Exception:
                pass

        # close underlying agent/transport
        agent = self._sessions.pop(peer_id, None)
        if agent and hasattr(agent, "close"):
            try:
                await agent.close()  # type: ignore
            except Exception:
                pass

        # drop queues and counters
        self._send_queues.pop(peer_id, None)
        self._queue_bytes.pop(peer_id, None)

    async def close(self) -> None:
        # cancel and await all drainers to avoid pending-task warnings
        for pid, task in list(self._drainers.items()):
            try:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            except Exception as e:
                log("drainer_cancel_error", local_id=self.local_id, peer_id=pid, error=str(e))
        self._drainers.clear()

        # close all transports
        for peer_id, agent in list(self._sessions.items()):
            if hasattr(agent, "close"):
                try:
                    await agent.close()  # type: ignore
                except Exception as e:
                    log("session_close_error", local_id=self.local_id, peer_id=peer_id, error=str(e))
        self._sessions.clear()

        # clear queues and counters
        self._send_queues.clear()
        self._queue_bytes.clear()

        incr("session_disconnects", 1)
        log("session_manager_closed", local_id=self.local_id)
