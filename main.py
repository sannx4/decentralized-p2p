# main.py
import asyncio
import uuid

from sync.bootstrap import attach_sync
from nat.ice_agent import ICEAgent
from crdt.engine import CRDTEngine


def load_or_create_id(path: str = "node_id.txt") -> str:
    """Keep a stable peer ID across restarts."""
    try:
        with open(path, "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        nid = "peer-" + uuid.uuid4().hex[:8]
        with open(path, "w") as f:
            f.write(nid)
        return nid


async def main():
    local_id = load_or_create_id()

    # Your real engine + ICE transport
    engine = CRDTEngine()
    agent = ICEAgent(name=local_id)

    # Wire gossip ↔ engine ↔ transport (EngineAdapter is applied inside)
    runtime = attach_sync(
        local_id=local_id,
        engine=engine,
        ice_agent=agent,
        debug=True,
        round_interval=0.3,
        fanout=1,
        room_id="doc:demo",  # optional: scope messages to a document/room
    )
    agent.set_runtime(runtime)

    # ---------------------------
    # Your signaling / connection flow
    # ---------------------------
    # 1) Side A:
    #    local_cands, ufrag, pwd = await agent.gather_candidates()
    #    send (local_cands, ufrag, pwd) to the other side via your signaling channel
    #
    # 2) Side B:
    #    await agent.start_connection(remote_cands, remote_ufrag, remote_pwd)
    #
    # After BOTH sides call start_connection(...), bind gossip over ICE:
    await agent.bind_gossip(local_id)

    # Keep process alive (replace with your app loop/UI)
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
