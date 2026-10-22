"""Reference solution: P2 limit order book (all three parts).

Heaps give O(log n) best price; cancels and re-prioritised modifies are lazy:
the stale heap entry stays and is skipped when it reaches the top.
"""

import heapq
from collections import defaultdict, namedtuple
from typing import Optional

Trade = namedtuple("Trade", "buy_id sell_id price qty")


class OrderBook:
    def __init__(self) -> None:
        self._heaps = {"BUY": [], "SELL": []}        # BUY: (-price, seq, id), SELL: (price, seq, id)
        self._orders: dict[str, list] = {}           # id -> [side, price, remaining, seq]
        self._levels = {"BUY": defaultdict(int), "SELL": defaultdict(int)}  # price -> total qty
        self._seq = 0

    # ---------------------------------------------------------- helpers
    def _push(self, order_id: str, side: str, price: float) -> int:
        self._seq += 1
        key = -price if side == "BUY" else price
        heapq.heappush(self._heaps[side], (key, self._seq, order_id))
        return self._seq

    def _top(self, side: str) -> Optional[str]:
        """Id of the best live order on a side, discarding stale entries."""
        heap = self._heaps[side]
        while heap:
            _, seq, oid = heap[0]
            o = self._orders.get(oid)
            if o is not None and o[3] == seq:
                return oid
            heapq.heappop(heap)
        return None

    def _reduce(self, oid: str, qty: int) -> None:
        o = self._orders[oid]
        o[2] -= qty
        lvl = self._levels[o[0]]
        lvl[o[1]] -= qty
        if lvl[o[1]] == 0:
            del lvl[o[1]]
        if o[2] == 0:
            del self._orders[oid]

    # ---------------------------------------------------------- Part 1
    def add(self, order_id: str, side: str, price: float, qty: int) -> list[Trade]:
        if side not in ("BUY", "SELL"):
            raise ValueError(f"bad side {side!r}")
        if qty <= 0:
            raise ValueError("qty must be positive")
        if order_id in self._orders:
            raise ValueError(f"duplicate order id {order_id!r}")

        other = "SELL" if side == "BUY" else "BUY"
        trades = []
        while qty > 0:
            rid = self._top(other)
            if rid is None:
                break
            rprice = self._orders[rid][1]
            if (side == "BUY" and price < rprice) or (side == "SELL" and price > rprice):
                break
            q = min(qty, self._orders[rid][2])
            buy, sell = (order_id, rid) if side == "BUY" else (rid, order_id)
            trades.append(Trade(buy, sell, rprice, q))   # trade at the resting order's price
            self._reduce(rid, q)
            qty -= q

        if qty > 0:
            seq = self._push(order_id, side, price)
            self._orders[order_id] = [side, price, qty, seq]
            self._levels[side][price] += qty
        return trades

    def best_bid(self) -> Optional[float]:
        oid = self._top("BUY")
        return None if oid is None else self._orders[oid][1]

    def best_ask(self) -> Optional[float]:
        oid = self._top("SELL")
        return None if oid is None else self._orders[oid][1]

    # ---------------------------------------------------------- Part 2
    def cancel(self, order_id: str) -> bool:
        if order_id not in self._orders:
            return False
        self._reduce(order_id, self._orders[order_id][2])
        return True

    def depth(self, side: str, levels: int) -> list[tuple[float, int]]:
        lvl = self._levels[side]
        prices = sorted(lvl, reverse=(side == "BUY"))[:levels]
        return [(p, lvl[p]) for p in prices]

    # ---------------------------------------------------------- Part 3
    def modify(self, order_id: str, new_qty: int) -> bool:
        o = self._orders.get(order_id)
        if o is None:
            return False
        if new_qty <= 0:
            return self.cancel(order_id)
        side, price, remaining, _ = o
        self._levels[side][price] += new_qty - remaining
        o[2] = new_qty
        if new_qty > remaining:                   # size up: loses time priority
            o[3] = self._push(order_id, side, price)
        return True
