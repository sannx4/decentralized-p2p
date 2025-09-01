# sync/bootstrap.py
import asyncio
from typing import Callable, Any, Optional
from p2p.proto import crdt_pb2, sync_pb2  # (ok to keep even if unused)
from .membership import HealthMonitor

from .engine_adapter import EngineAdapter
from .peer_manager import PeerManager
from .gossip_sync import GossipSync


class SyncRuntime:
    """
    Bundles PeerManager + GossipSync and wires them to your ICE/WebRTC transport.
    """
    def __init__(
        self,
        local_id: str,
        engine: Any,
        send_bytes: Callable[[str, bytes], None],  # (peer_id, payload)
        round_interval: float = 1.5,
        fanout: int = 3,
        max_bytes: int = 64 * 1024,
        max_ops: int = 2048,
        debug: bool = False,
        room_id: Optional[str] = None,
    ):
        self.pm = PeerManager()
        self.adapter = EngineAdapter(engine)
        self.gossip = GossipSync(
            local_id=local_id,
            engine=self.adapter,
            peer_manager=self.pm,
            send_fn=send_bytes,
            round_interval=round_interval,
            fanout=fanout,
            max_bytes=max_bytes,
            max_ops=max_ops,
            room_id=room_id,
        )
        self.gossip.debug = debug

        # Health/liveness monitor
        self.health = HealthMonitor(self.pm, check_every=2.0, stale_after=10.0)

    async def start(self):
        await self.gossip.start()
        await self.health.start()

    async def on_channel_open(self, peer_id: str, dc: Any):
        # Register peer in the PeerManager so send_fn can find the DC
        await self.pm.add_peer(peer_id, dc)

        # Hook inbound messages to Gossip
        async def _on_msg(data: bytes):
            await self.pm.mark_seen(peer_id)
            await self.gossip.on_message(peer_id, data)

        # Hook close to cleanup
        async def _on_close():
            await self.pm.remove_peer(peer_id)

        # Adapt to your DC event API:
        # If your DC supports decorators: @dc.on("message") / @dc.on("close")
        # otherwise assign handlers explicitly:
        try:
            # aiortc-style
            @dc.on("message")
            async def _msg_handler(msg):
                # aiortc may send str or bytes; we only handle bytes
                if isinstance(msg, (bytes, bytearray)):
                    await _on_msg(bytes(msg))

            @dc.on("close")
            async def _close_handler():
                await _on_close()
        except Exception:
            # Fallback to attribute handlers
            if hasattr(dc, "on_message"):
                dc.on_message = _on_msg
            if hasattr(dc, "on_close"):
                dc.on_close = _on_close

    async def stop(self):
        await self.health.stop()
        await self.gossip.stop()


def attach_sync(
    *,
    local_id: str,
    engine: Any,
    ice_agent: Any,
    debug: bool = False,
    round_interval: float = 1.5,
    fanout: int = 3,
    room_id: Optional[str] = None,
) -> SyncRuntime:
    """
    Call this once at startup. You supply:
      - local_id: your stable peer ID (uuid/ulid)
      - engine: your CRDT engine instance
      - ice_agent: your transport, must let us:
          • send bytes to a peer_id
          • get callbacks when a datachannel is opened for a peer
    Returns a SyncRuntime you can keep if you want to stop() later.
    """

    # 1) define how to send bytes via your ICE layer
    def send_bytes(peer_id: str, payload: bytes):
        # If your agent gives you a direct send(peer_id, bytes), use that:
        if hasattr(ice_agent, "send"):
            ice_agent.send(peer_id, payload)
            return

        # Otherwise, look up the datachannel and call its .send
        dc = None
        if hasattr(ice_agent, "get_datachannel"):
            dc = ice_agent.get_datachannel(peer_id)
        if dc and hasattr(dc, "send"):
            dc.send(payload)
        else:
            # Last resort: if you stored channels in a dict on the agent
            try:
                dc = ice_agent.channels[peer_id]
                dc.send(payload)
            except Exception:
                raise RuntimeError(
                    "No way to send bytes to peer_id; expose send(peer_id, bytes) or get_datachannel()."
                )

    runtime = SyncRuntime(
        local_id=local_id,
        engine=engine,
        send_bytes=send_bytes,
        round_interval=round_interval,
        fanout=fanout,
        debug=debug,
        room_id=room_id,
    )

    # 2) subscribe to "datachannel open" from your ICE layer
    def on_datachannel_open(peer_id: str, dc):
        asyncio.create_task(runtime.on_channel_open(peer_id, dc))

    # Try to register using common patterns:
    if hasattr(ice_agent, "on_datachannel"):  # aiortc RTCPeerConnection.on("datachannel")
        @ice_agent.on_datachannel  # if your agent supports decorator
        async def _on_dc(peer_id: str, dc):
            on_datachannel_open(peer_id, dc)
    elif hasattr(ice_agent, "on"):
        # Generic event emitter pattern
        ice_agent.on("datachannel", on_datachannel_open)
    else:
        # Otherwise, expect the app to call runtime.on_channel_open(peer_id, dc)
        pass

    # 3) start the gossip + health loops
    asyncio.create_task(runtime.start())
    return runtime
