from __future__ import annotations
from typing import Dict

class DedupeWindow:
    """
    Per-sender sliding window for (from_id, seq_no) duplicate/drop checks.
    Keeps last_seq and a 256-bit bitmap for recent deliveries.
    """
    __slots__ = ("last_seq", "bitmap")

    def __init__(self) -> None:
        self.last_seq: int = 0
        self.bitmap: int = 0  # LSB = last_seq, next = last_seq-1, etc.

    def seen(self, seq_no: int) -> bool:
        if seq_no <= 0:
            return False
        if seq_no > self.last_seq:
            # advance window forward by delta
            shift = seq_no - self.last_seq
            if shift >= 256:
                self.bitmap = 1  # only mark this seq
            else:
                self.bitmap = ((self.bitmap << shift) | 1) & ((1 << 256) - 1)
            self.last_seq = seq_no
            return False  # first time seeing it
        else:
            # within window?
            offset = self.last_seq - seq_no
            if offset >= 256:
                return True  # too old; treat as seen
            mask = 1 << offset
            already = (self.bitmap & mask) != 0
            self.bitmap |= mask
            return already

class DedupeTable:
    """
    Maintains a DedupeWindow per sender.
    """
    def __init__(self) -> None:
        self._by_sender: Dict[str, DedupeWindow] = {}

    def already_seen(self, from_id: str, seq_no: int) -> bool:
        win = self._by_sender.get(from_id)
        if win is None:
            win = self._by_sender[from_id] = DedupeWindow()
        return win.seen(seq_no)
