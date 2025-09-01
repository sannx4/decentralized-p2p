import asyncio
from typing import Optional, Callable

_HANDSHAKE_TIMEOUT = 5.0  # seconds
_MAX_READ = 64 * 1024


class TCPAgent:
    """
    Minimal stream-based transport with a tiny handshake:
      each side sends: b"ID:<peer_id>\\n" once on connect/accept.
    """

    def __init__(self, local_id: str):
        self.local_id = local_id
        self.remote_id: Optional[str] = None
        self._on_message: Optional[Callable[[bytes], None]] = None
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._rx_task: Optional[asyncio.Task] = None
        self._closed = False

    # ------------- public API -------------
    def set_on_message(self, cb: Callable[[bytes], None]) -> None:
        self._on_message = cb

    async def connect(self, host: str, port: int) -> None:
        reader, writer = await asyncio.open_connection(host, port)
        await self._handshake_client(reader, writer)

    async def close(self) -> None:
     self._closed = True
     if self._rx_task:
        try:
            self._rx_task.cancel()
            try:
                await self._rx_task
            except asyncio.CancelledError:
                pass
        finally:
            self._rx_task = None
     try:
        if self._writer:
            self._writer.close()
            await self._writer.wait_closed()
     except Exception:
        pass
     self._reader = None
     self._writer = None

    async def send(self, data: bytes) -> None:
        if not self._writer:
            return
        self._writer.write(data)
        await self._writer.drain()

    # ------------- server side -------------
    async def serve_accepted(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> str:
        """Server path: perform handshake and start RX loop. Returns remote_id."""
        await self._handshake_server(reader, writer)
        return self.remote_id or ""

    # ------------- internals -------------
    async def _handshake_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._reader, self._writer = reader, writer
        # send my ID
        writer.write(f"ID:{self.local_id}\n".encode())
        await writer.drain()
        # read their ID
        line = await asyncio.wait_for(reader.readline(), timeout=_HANDSHAKE_TIMEOUT)
        if not line.startswith(b"ID:"):
            raise RuntimeError("bad handshake (server)")
        self.remote_id = line[3:].strip().decode()  # after "ID:"
        self._start_rx_loop()

    async def _handshake_server(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._reader, self._writer = reader, writer
        # read their ID first
        line = await asyncio.wait_for(reader.readline(), timeout=_HANDSHAKE_TIMEOUT)
        if not line.startswith(b"ID:"):
            raise RuntimeError("bad handshake (client)")
        self.remote_id = line[3:].strip().decode()
        # send my ID
        writer.write(f"ID:{self.local_id}\n".encode())
        await writer.drain()
        self._start_rx_loop()

    def _start_rx_loop(self) -> None:
        if self._rx_task:
            return
        self._rx_task = asyncio.create_task(self._rx_loop())

    async def _rx_loop(self) -> None:
        try:
            while not self._closed and self._reader:
                data = await self._reader.read(_MAX_READ)
                if not data:
                    break
                if self._on_message:
                    try:
                        self._on_message(data)
                    except Exception:
                        pass
        except asyncio.CancelledError:
            return
        except Exception:
            await asyncio.sleep(0)
