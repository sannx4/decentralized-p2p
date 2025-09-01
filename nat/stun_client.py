# nat/stun_client.py
import asyncio
import aioice

async def run_stun_discovery():
    connection = aioice.Connection(ice_controlling=True)
    await connection.gather_candidates()
    
    for candidate in connection.local_candidates:
        print(f"[STUN] Found candidate: {candidate}")

    await connection.close()

if __name__ == "__main__":
    asyncio.run(run_stun_discovery())
