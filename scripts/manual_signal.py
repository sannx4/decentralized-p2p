# scripts/manual_signal.py
import argparse
import asyncio
import json
import uuid
import base64
import pickle
from typing import Optional

from nat.ice_agent import ICEAgent
from sync.bootstrap import attach_sync
from crdt.engine import CRDTEngine
from crdt.local_ops_helper import LocalOpsHelper


# ---------- candidate (de)serialization ----------
def _encode_candidates(cands):
    # serialize Candidate objects to base64 (ASCII-safe)
    return [base64.b64encode(pickle.dumps(c)).decode("ascii") for c in cands]

def _decode_candidates(b64_list):
    out = []
    for i, b in enumerate(b64_list):
        try:
            out.append(pickle.loads(base64.b64decode(b)))
        except Exception as e:
            raise ValueError(f"Failed to decode candidate #{i}: {e}")
    return out
# -------------------------------------------------


async def run(role: str, remote_blob: Optional[str], remote_file: Optional[str]):
    local_id = f"peer-{uuid.uuid4().hex[:6]}"
    engine = CRDTEngine(peer_id=local_id)
    helper = LocalOpsHelper(engine)
    agent  = ICEAgent(name=local_id)

    runtime = attach_sync(
        local_id=local_id,
        engine=engine,
        ice_agent=agent,
        debug=True,
        round_interval=0.3,
        fanout=1,
    )
    agent.set_runtime(runtime)

    # Gather local ICE info
    cands, ufrag, pwd = await agent.gather_candidates()
    local_blob = json.dumps({"cands": _encode_candidates(cands), "ufrag": ufrag, "pwd": pwd})

    # Helper to load remote blob from file/stdin/arg
    def _get_remote_blob(prompt: str) -> str:
        if remote_file:
            with open(remote_file, "r") as f:
                return f.read().strip()
        if remote_blob:
            return remote_blob.strip()
        print(prompt)
        return input().strip()

    # ------- NEW: tiny op-log printer (non-intrusive) -------
    async def log_printer():
        def _dec(p):
            try:
                return p.decode()
            except Exception:
                return repr(p)
        while True:
            payloads = [_dec(op.payload) for op in getattr(engine, "op_log", [])]
            print(f"[{role.upper()} LOG]:", payloads)
            await asyncio.sleep(3.0)
    # --------------------------------------------------------

    if role.upper() == "A":
        print("\n--- LOCAL (send this to B) ---")
        print(local_blob)

        rb = _get_remote_blob("\n--- Paste REMOTE blob from B and press Enter ---")
        remote = json.loads(rb)
        rcands = _decode_candidates(remote["cands"])
        rufrag, rpwd = remote["ufrag"], remote["pwd"]

        await agent.start_connection(rcands, rufrag, rpwd)
        await agent.bind_gossip(local_id)

        # A makes two local ops
        helper.make_local_op(file_id="doc", payload=b"a1")
        helper.make_local_op(file_id="doc", payload=b"a2")

        async def vv_printer():
            while True:
                print(f"[{role.upper()} VV]:", runtime.adapter.clock())
                await asyncio.sleep(2.0)
        asyncio.create_task(vv_printer())
        asyncio.create_task(log_printer())   # <— ADDED

        await asyncio.Event().wait()

    elif role.upper() == "B":
        # B prints its blob FIRST so A can paste it back
        print("\n--- LOCAL (send this to A) ---")
        print(local_blob)

        # Now B waits for A's blob
        rb = _get_remote_blob("Paste REMOTE blob from A and press Enter:")
        remote = json.loads(rb)
        rcands = _decode_candidates(remote["cands"])
        rufrag, rpwd = remote["ufrag"], remote["pwd"]

        await agent.start_connection(rcands, rufrag, rpwd)
        await agent.bind_gossip(local_id)

        # Optional: B makes one local op so you can see cross-sync both ways
        helper.make_local_op(file_id="doc", payload=b"b1")

        async def vv_printer():
            while True:
                print(f"[{role.upper()} VV]:", runtime.adapter.clock())
                await asyncio.sleep(2.0)
        asyncio.create_task(vv_printer())
        asyncio.create_task(log_printer())   # <— ADDED

        await asyncio.Event().wait()
    else:
        print("role must be A or B")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--role", required=True, help="A or B")
    ap.add_argument("--remote", help="JSON blob from the other side (string)")
    ap.add_argument("--remote-file", help="Path to a file containing the remote JSON blob")
    args = ap.parse_args()
    asyncio.run(run(args.role, args.remote, args.remote_file))
