# P2: Limit order book (~50 min)

The classic exchange problem. No finance knowledge is needed beyond these definitions:

* An order is `(order_id, side, price, qty)`, where `side` is `"BUY"` or `"SELL"`.
* A BUY order and a SELL order **cross** when buy price ≥ sell price. Crossing orders trade.
* Orders that don't fully trade **rest** in the book and wait.
* **Price-time priority:** the best price trades first (highest buy, lowest sell). Within a price, the order that
  arrived first trades first.

Work in `p2_order_book.py` (a `Trade` namedtuple is provided). Test with `python run.py p2 --part 1`.

## Part 1

Implement `OrderBook` with these methods:

* `add(order_id, side, price, qty) -> list[Trade]`. Match the new (incoming) order against resting orders on the
  other side while they cross, following price-time priority, and return the trades in order. Each trade happens at
  the **resting order's price**. `Trade(buy_id, sell_id, price, qty)` always puts the buy id first. Whatever
  quantity remains rests in the book.
* `best_bid() -> float | None` returns the highest resting buy price. `best_ask() -> float | None` returns the
  lowest resting sell price.
* Raise `ValueError` if `qty <= 0`, if the side is invalid, or if `order_id` is already resting in the book.

---
> ⛔ **Stop.** Get Part 1 passing before reading on.
---

## Part 2

* `cancel(order_id) -> bool` removes a resting order. Return `False` if it's unknown, already cancelled or fully
  filled.
* `depth(side, levels) -> list[tuple[float, int]]` returns the top `levels` price levels with the **total** resting
  quantity at each. Bids go highest first, asks lowest first.

If your Part 1 data structure makes cancel slow, say so and explain what you'd change.

---
> ⛔ **Stop.**
---

## Part 3

* `modify(order_id, new_qty) -> bool`:
  * Reducing the quantity **keeps** time priority.
  * Increasing it **loses** priority: the order goes to the back of its price level.
  * `new_qty <= 0` means cancel.
  * Unknown ids return `False`.
* **Performance.** 100k mixed add, cancel and modify operations must finish in a few seconds. A sorted list that you
  re-sort on every add won't make it. Which structures give you O(log n)?

## Discussion questions

* Why is lazy deletion (marking cancelled orders, then skipping them when they reach the top of a heap) a reasonable
  trade-off? When does it hurt?
* Floats as prices are a bug waiting to happen. What would you use instead? (Integer ticks.)
* How would you test a matching engine? Think about invariants: is the book ever crossed after `add` returns, and
  is quantity conserved?
