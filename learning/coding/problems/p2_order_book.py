"""P2: limit order book. Read p2_order_book.md one part at a time.

Add cancel/depth (Part 2) and modify (Part 3) to OrderBook when you reach them.
"""

from collections import namedtuple
from typing import Optional

Trade = namedtuple("Trade", "buy_id sell_id price qty")


class OrderBook:
    def __init__(self) -> None:
        pass

    def add(self, order_id: str, side: str, price: float, qty: int) -> list[Trade]:
        raise NotImplementedError

    def best_bid(self) -> Optional[float]:
        raise NotImplementedError

    def best_ask(self) -> Optional[float]:
        raise NotImplementedError
