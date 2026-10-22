"""Reference solution: P4 rolling VWAP over a trade stream (all three parts)."""

import bisect
from collections import defaultdict, deque
from typing import Optional


class RollingVWAP:
    def __init__(self, window_ms: int, max_lateness_ms: int = 0) -> None:
        self.window_ms = window_ms
        self.max_lateness_ms = max_lateness_ms
        # Per symbol: trades sorted by ts as a list, plus a start offset (cheap left-eviction).
        self._trades: dict[str, list[tuple[int, int, float, int]]] = defaultdict(list)
        self._start: dict[str, int] = defaultdict(int)
        self._pv: dict[str, float] = defaultdict(float)
        self._qty: dict[str, int] = defaultdict(int)
        self._seq = 0
        self._max_ts: Optional[int] = None
        self._seen_ids: set[str] = set()
        self._id_queue: deque[tuple[int, str]] = deque()  # (ts, id) in arrival order
        self.rejected_late = 0

    # ---------------------------------------------------------- Parts 1-3
    def on_trade(self, ts: int, symbol: str, price: float, qty: int, trade_id: Optional[str] = None) -> bool:
        if self._max_ts is not None and ts < self._max_ts - self.max_lateness_ms:
            self.rejected_late += 1
            return False
        if trade_id is not None:
            if trade_id in self._seen_ids:
                return False
            self._seen_ids.add(trade_id)
            self._id_queue.append((ts, trade_id))
        self._max_ts = ts if self._max_ts is None else max(self._max_ts, ts)
        self._forget_ids()

        self._seq += 1
        # A late trade older than the last cutoff lands at the front and is evicted by the next query.
        bisect.insort(self._trades[symbol], (ts, self._seq, price, qty), lo=self._start[symbol])
        self._pv[symbol] += price * qty
        self._qty[symbol] += qty
        return True

    def vwap(self, symbol: str, now: int) -> Optional[float]:
        trades, i = self._trades[symbol], self._start[symbol]
        cutoff = now - self.window_ms                      # window is (now - window_ms, now]
        while i < len(trades) and trades[i][0] <= cutoff:
            _, _, price, qty = trades[i]
            self._pv[symbol] -= price * qty
            self._qty[symbol] -= qty
            i += 1
        if i > 1024 and i > len(trades) // 2:              # compact occasionally: amortised O(1)
            del trades[:i]
            i = 0
        self._start[symbol] = i
        if self._qty[symbol] == 0:
            self._pv[symbol] = 0.0                         # reset float drift when the window empties
            return None
        return self._pv[symbol] / self._qty[symbol]

    # ---------------------------------------------------------- Part 2
    def _forget_ids(self) -> None:
        """A replay arrives within window_ms of the original, so older ids can be dropped."""
        horizon = self._max_ts - self.window_ms - self.max_lateness_ms
        while self._id_queue and self._id_queue[0][0] < horizon:
            self._seen_ids.discard(self._id_queue.popleft()[1])

    def tracked_ids(self) -> int:
        return len(self._seen_ids)
