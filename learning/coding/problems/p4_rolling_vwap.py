"""P4: rolling VWAP over a trade stream. Read p4_rolling_vwap.md one part at a time.

Parts 2 and 3 change the signatures of __init__ and on_trade, and add tracked_ids.
"""

from typing import Optional


class RollingVWAP:
    def __init__(self, window_ms: int) -> None:
        self.window_ms = window_ms

    def on_trade(self, ts: int, symbol: str, price: float, qty: int) -> None:
        raise NotImplementedError

    def vwap(self, symbol: str, now: int) -> Optional[float]:
        raise NotImplementedError
