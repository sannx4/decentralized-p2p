import asyncio
import argparse
import json
import time
from typing import Dict, Optional, List

from net.tcp_agent import TCPAgent
from net.session_manager import SessionManager
from net.peer_directory import PeerDirectory, PeerInfo, PeerStatus, HEARTBEAT_PERIOD_MS
from sync.gossip import GossipEngine
from util.metrics import snapshot as metrics_snapshot
from util.log import log
from util.storage import save_snapshot, load_snapshot


class KVCRDT:
    def __init__(self, peer_id: str):
        self.peer_id = peer_id
        self.state: Dict[str, str] = {}
        self._pending: List[bytes] = []
        snap = load_snapshot(f".data/{peer_id}.json")
        if isinstance(snap, dict):
            self.state.update({str(k): str(v) for k, v in snap.items()})

    def set(self, key: str, value: str) -> None:
        delta = f"set:{key}={value}".encode()
        self._apply(delta)
        self._pending.append(delta)

    def collect_deltas(self) -> List[bytes]:
        out = self._pending
        self._pending = []
        return out

    def apply_remote_delta(self, d: bytes) -> None:
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

    def persist_now(self) -> None:
        save_snapshot(f".data/{self.peer_id}.json", self.state)


async def http_server(
    port: int,
    engine: KVCRDT,
    directory: PeerDirectory,
    connect_cb,  # async function scheme:str -> str (message)
):
    async def handle_reader(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            req = await reader.read(4096)
            line = req.split(b"\r\n", 1)[0].decode("utf-8", "ignore")
            parts = line.split(" ")
            path = parts[1] if len(parts) > 1 else "/"

            if path.startswith("/metrics"):
                body = json.dumps(metrics_snapshot(), separators=(",", ":")).encode()

            elif path.startswith("/state"):
                alive = [p.peer_id for p in directory.all_peers() if p.status == PeerStatus.ALIVE]
                body = json.dumps({"state": engine.state, "alive": alive}, separators=(",", ":")).encode()

            elif path.startswith("/peers"):
                allp = [{"id": p.peer_id, "last_seen_ms": p.last_seen_ms, "status": p.status.name}
                        for p in directory.all_peers()]
                body = json.dumps({"peers": allp}, separators=(",", ":")).encode()

            elif path.startswith("/get"):
                from urllib.parse import urlparse, parse_qs
                qs = parse_qs(urlparse(path).query)
                k = qs.get("k", [None])[0]
                v = engine.state.get(k) if k else None
                body = json.dumps({"ok": True, "k": k, "v": v}, separators=(",", ":")).encode()

            elif path.startswith("/set"):
                from urllib.parse import urlparse, parse_qs
                qs = parse_qs(urlparse(path).query)
                k = qs.get("k", [None])[0]
                v = qs.get("v", [None])[0]
                if k is not None and v is not None:
                    engine.set(k, v)
                    body = json.dumps({"ok": True, "set": {k: v}}, separators=(",", ":")).encode()
                else:
                    body = b'{"ok":false,"err":"missing k or v"}'

            elif path.startswith("/join"):
                # /join?target=tcp://host:port
                from urllib.parse import urlparse, parse_qs
                qs = parse_qs(urlparse(path).query)
                target = qs.get("target", [None])[0]
                if not target:
                    body = b'{"ok":false,"err":"missing target"}'
                else:
                    try:
                        msg = await connect_cb(target)
                        body = json.dumps({"ok": True, "msg": msg}, separators=(",", ":")).encode()
                    except Exception as e:
                        body = json.dumps({"ok": False, "err": str(e)}, separators=(",", ":")).encode()

            else:
                body = b'{"ok":true}'

            headers = (
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: application/json\r\n"
                f"Content-Length: {len(body)}\r\n"
                "Connection: close\r\n"
                "\r\n"
            ).encode()
            writer.write(headers + body)
            await writer.drain()

        except Exception as e:
            print("[http] error:", e)
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    return await asyncio.start_server(handle_reader, "0.0.0.0", port)


async def periodic_heartbeats(ge: GossipEngine, period_ms: int):
    while True:
        await ge.send_heartbeat()
        await asyncio.sleep(period_ms / 1000.0)


async def periodic_autosave(engine: KVCRDT, period_sec: int = 5):
    while True:
        await asyncio.sleep(period_sec)
        try:
            engine.persist_now()
        except Exception:
            pass


async def start_tcp_listener(local_id: str, port: int, sessions: SessionManager, directory: PeerDirectory):
    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        agent = TCPAgent(local_id)
        try:
            remote_id = await agent.serve_accepted(reader, writer)
        except Exception:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            return
        sessions.add_session(remote_id, agent)
        now_ms = int(time.time() * 1000)
        directory.add_or_update(PeerInfo(peer_id=remote_id, last_seen_ms=now_ms, status=PeerStatus.ALIVE))
        log("tcp_inbound_link", local=local_id, remote=remote_id, port=port)

    server = await asyncio.start_server(handle, "0.0.0.0", port)
    log("tcp_listen", local=local_id, port=port)
    return server


async def run_node(peer_id: str,
                   http_port: int,
                   token: Optional[str],
                   tcp_listen: Optional[int],
                   connect: List[str]):
    print(f"[node] starting id={peer_id} http=:{http_port}", flush=True)

    engine = KVCRDT(peer_id)
    sessions = SessionManager(local_id=peer_id, cluster_token=token)
    directory = PeerDirectory()
    gossip = GossipEngine(engine=engine, sessions=sessions, directory=directory, peer_id=peer_id)

    gossip.start()
    asyncio.create_task(periodic_heartbeats(gossip, HEARTBEAT_PERIOD_MS))
    asyncio.create_task(periodic_autosave(engine, 5))

    async def connect_cb(target: str) -> str:
     if target.startswith("tcp://"):
        addr = target[len("tcp://"):]
        host, port_s = addr.split(":", 1)
        agent = TCPAgent(peer_id)
        await agent.connect(host, int(port_s))
        remote_id = agent.remote_id or addr

        if remote_id in sessions.active_peers():  # dedupe outbound
            await agent.close()
            return f"already linked {peer_id} <-> {remote_id}"

        sessions.add_session(remote_id, agent)
        now_ms = int(time.time() * 1000)
        directory.add_or_update(PeerInfo(peer_id=remote_id, last_seen_ms=now_ms, status=PeerStatus.ALIVE))
        log("tcp_outbound_link", local=peer_id, remote=remote_id, host=host, port=int(port_s))
        return f"linked {peer_id} -> {remote_id} @ {host}:{port_s}"
    raise RuntimeError("unsupported scheme (use tcp://host:port)")


    http_srv = await http_server(http_port, engine, directory, connect_cb)
    log("node_http_listen", peer=peer_id, port=http_port)

    tcp_srv = None
    if tcp_listen:
        tcp_srv = await start_tcp_listener(peer_id, tcp_listen, sessions, directory)

    # initial outbound connects (from --connect args)
    for target in connect:
        try:
            msg = await connect_cb(target)
            print("[node]", msg)
        except Exception as e:
            print(f"[warn] connect failed {target}: {e}")

    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        if tcp_srv:
            tcp_srv.close()
            await tcp_srv.wait_closed()
        http_srv.close()
        await http_srv.wait_closed()
        engine.persist_now()
        await sessions.close()
        print("[node] stopped", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--peer-id", required=True)
    ap.add_argument("--http-port", type=int, default=8080)
    ap.add_argument("--token", default=None)
    ap.add_argument("--tcp-listen", type=int, default=None, help="listen for TCP peers on this port")
    ap.add_argument("--connect", nargs="*", default=[], help='targets like tcp://host:port')
    args = ap.parse_args()
    asyncio.run(run_node(args.peer_id, args.http_port, args.token, args.tcp_listen, args.connect))


if __name__ == "__main__":
    main()
