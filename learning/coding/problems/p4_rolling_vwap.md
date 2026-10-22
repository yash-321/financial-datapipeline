# P4: Rolling VWAP over a trade stream (~45 min)

Trades stream in as `(ts, symbol, price, qty)`, where `ts` is epoch milliseconds. VWAP (volume-weighted average
price) is `Σ(price × qty) / Σ(qty)` over a set of trades.

Work in `p4_rolling_vwap.py`. Test with `python run.py p4 --part 1`.

## Part 1

`RollingVWAP(window_ms)` has two methods:

* `on_trade(ts, symbol, price, qty)` records a trade.
* `vwap(symbol, now) -> float | None` returns the VWAP of that symbol's trades with `now - window_ms < ts <= now`,
  or `None` if there are none.

Assume trades arrive in non-decreasing `ts`, and that queries use a non-decreasing `now` that is ≥ every `ts` seen
so far. **Both operations should be amortised O(1).** Don't re-scan the whole history on each query.

---
> ⛔ **Stop.** Get Part 1 passing before reading on.
---

## Part 2

The feed is at-least-once, so trades can be **replayed**. `on_trade` gains an optional `trade_id` and returns
`bool`: `True` if the trade was accepted, `False` for a duplicate id, which must be ignored.

Memory must stay bounded. A replay always arrives within `window_ms` of the original, so you may forget older ids.
Add `tracked_ids() -> int` returning how many ids you're currently remembering.

---
> ⛔ **Stop.**
---

## Part 3

Trades can now arrive **out of order**, up to `max_lateness_ms` behind the largest `ts` seen so far:
`RollingVWAP(window_ms, max_lateness_ms=0)`.

* A trade more than `max_lateness_ms` behind the max `ts` seen is **rejected**: return `False` and increment
  `self.rejected_late`.
* Accepted late trades must count in every query whose window contains them.
* `now` is still ≥ every `ts` seen and still non-decreasing.

## Discussion questions

* Running float sums drift when you add and subtract millions of times. How would you notice, and what would you
  do? (Integer ticks, periodic recompute, or Kahan summation.)
* Part 3 breaks the "append to the right, evict from the left" deque trick. What did you replace it with, and what
  does it cost?
* Where does this logic live in a real pipeline (Kafka consumer, Flink/Spark window, watermark)? How does
  `max_lateness_ms` relate to a watermark?
