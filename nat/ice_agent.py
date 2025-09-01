# nat/ice_agent.py
import aioice
import asyncio
import logging
import pickle

from p2p.proto import crdt_pb2
from crdt.types import CRDTOperation, OperationType
from crdt.engine import OperationType as LocalOperationType  # (ok if unused)

logging.basicConfig(level=logging.INFO)

class ICEAgent:
    def __init__(self, name: str, controlling: bool = True):
        self.name = name
        self.connection = aioice.Connection(
            ice_controlling=controlling,
            # STUN helps beyond localhost; safe to keep on localhost too
            stun_server=('stun.l.google.com', 19302),
        )

        # runtime + remote identity
        self._runtime = None
        self._remote_id = None

        # Tiny DC adapter so runtime can call .send(bytes) non-async
        class _DCAdapter:
            def __init__(self, conn: aioice.Connection):
                self._conn = conn
            def send(self, payload: bytes):
                asyncio.create_task(self._conn.send(payload))

        self._dc_adapter = _DCAdapter(self.connection)

    def set_runtime(self, runtime):
        """Call once after you create runtime in main.py."""
        self._runtime = runtime

    async def gather_candidates(self):
        await self.connection.gather_candidates()
        return (
            self.connection.local_candidates,
            self.connection.local_username,
            self.connection.local_password,
        )

    async def start_connection(self, remote_candidates, remote_ufrag, remote_pwd):
        self.connection.remote_username = remote_ufrag
        self.connection.remote_password = remote_pwd
        for candidate in remote_candidates:
            await self.connection.add_remote_candidate(candidate)
        # IMPORTANT: signal end-of-candidates to aioice
        await self.connection.add_remote_candidate(None)
        await self.connection.connect()
        logging.info(f"[{self.name}] Connected!")

    # ---------------- Existing CRDT helpers ----------------
    async def send_crdt_operation(self, crdt_op: CRDTOperation):
        msg = crdt_pb2.CRDTOperation(
            file_id=crdt_op.file_id,
            operation_type=crdt_op.operation_type.value,  # int enum value
            payload=crdt_op.payload,
            logical_timestamp=crdt_op.logical_timestamp,
            peer_id=crdt_op.peer_id,
            parent_hash=crdt_op.parent_hash,
        )
        await self.connection.send(msg.SerializeToString())

    async def receive_crdt_operation(self) -> CRDTOperation:
        data = await self.connection.recv()
        msg = crdt_pb2.CRDTOperation()
        msg.ParseFromString(data)
        return CRDTOperation(
            file_id=msg.file_id,
            operation_type=OperationType(msg.operation_type),
            payload=msg.payload,
            logical_timestamp=msg.logical_timestamp,
            peer_id=msg.peer_id,
            parent_hash=msg.parent_hash,
        )

    async def send_data(self, obj):
        """Send any general Python object (like dicts)."""
        data = pickle.dumps(obj)
        await self.connection.send(data)

    async def receive_data(self):
        """Receive any general Python object."""
        data = await self.connection.recv()
        return pickle.loads(data)

    async def close(self):
        await self.connection.close()

    # ---------------- Gossip wiring ----------------
    def send(self, peer_id: str, payload: bytes):
        """Used by attach_sync's send_bytes(). Single-peer agent: ignore peer_id."""
        asyncio.create_task(self.connection.send(payload))

    async def bind_gossip(self, local_id: str):
        """
        Call right after start_connection(...).
        Handshake peer IDs, register a pseudo-DC with runtime, and start forwarding bytes.
        """
        if not self._runtime:
            raise RuntimeError("Call agent.set_runtime(runtime) before bind_gossip().")

        # Simple identity handshake: send ours, then read theirs
        await self.connection.send(local_id.encode("utf-8"))
        remote_id_bytes = await self.connection.recv()
        self._remote_id = remote_id_bytes.decode("utf-8")

        # Tell runtime a 'datachannel' is open so it can route sends
        await self._runtime.on_channel_open(self._remote_id, self._dc_adapter)

        # Start forwarding incoming bytes to GossipSync
        asyncio.create_task(self._recv_loop())

    async def _recv_loop(self):
        try:
            while True:
                data = await self.connection.recv()
                # Mark peer healthy & forward raw bytes to gossip
                if self._runtime and self._remote_id:
                    await self._runtime.pm.mark_seen(self._remote_id)
                    await self._runtime.gossip.on_message(self._remote_id, data)
        except Exception as e:
            logging.warning(f"[{self.name}] recv loop stopped: {e}")
