# sync/membership.py
import asyncio
import time
from typing import Optional

class HealthMonitor:
    def __init__(self, peer_manager, check_every: float = 2.0, stale_after: float = 10.0):
        self.pm = peer_manager
        self.check_every = check_every
        self.stale_after = stale_after
        self._task: Optional[asyncio.Task] = None

    async def start(self):
        if not self._task:
            self._task = asyncio.create_task(self._loop())

    async def stop(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _loop(self):
        while True:
            now = time.time()
            for p in self.pm.all():
                p.healthy = (now - p.last_seen) <= self.stale_after
            await asyncio.sleep(self.check_every)

