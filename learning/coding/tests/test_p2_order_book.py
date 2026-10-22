"""Tests for P2. Each test takes the module under test as `m`."""

import random
import time


def _raises(exc, fn, *args):
    try:
        fn(*args)
    except exc:
        return True
    return False


# ---------------------------------------------------------------- Part 1
def test_part1_empty_book(m):
    b = m.OrderBook()
    assert b.best_bid() is None and b.best_ask() is None


def test_part1_resting_orders_set_best_prices(m):
    b = m.OrderBook()
    assert b.add("b1", "BUY", 99.0, 10) == []
    assert b.add("s1", "SELL", 101.0, 5) == []
    assert (b.best_bid(), b.best_ask()) == (99.0, 101.0)
    b.add("b2", "BUY", 100.0, 1)
    assert b.best_bid() == 100.0


def test_part1_trade_at_resting_price(m):
    b = m.OrderBook()
    b.add("s1", "SELL", 101.0, 5)
    assert b.add("b1", "BUY", 102.0, 3) == [m.Trade("b1", "s1", 101.0, 3)]
    assert b.best_ask() == 101.0 and b.best_bid() is None


def test_part1_sweep_levels_and_rest_remainder(m):
    b = m.OrderBook()
    for oid, px in (("s1", 101.0), ("s2", 102.0), ("s3", 103.0)):
        b.add(oid, "SELL", px, 5)
    assert b.add("b1", "BUY", 102.0, 12) == [m.Trade("b1", "s1", 101.0, 5), m.Trade("b1", "s2", 102.0, 5)]
    assert (b.best_bid(), b.best_ask()) == (102.0, 103.0)


def test_part1_time_priority_within_level(m):
    b = m.OrderBook()
    b.add("s1", "SELL", 100.0, 5)
    b.add("s2", "SELL", 100.0, 5)
    assert b.add("b1", "BUY", 100.0, 7) == [m.Trade("b1", "s1", 100.0, 5), m.Trade("b1", "s2", 100.0, 2)]


def test_part1_sell_aggressor_buy_id_first(m):
    b = m.OrderBook()
    b.add("b1", "BUY", 100.0, 5)
    b.add("b2", "BUY", 99.0, 5)
    assert b.add("s1", "SELL", 99.0, 8) == [m.Trade("b1", "s1", 100.0, 5), m.Trade("b2", "s1", 99.0, 3)]
    assert b.best_bid() == 99.0


def test_part1_rejects_bad_input(m):
    b = m.OrderBook()
    b.add("b1", "BUY", 100.0, 5)
    assert _raises(ValueError, b.add, "x", "BUY", 100.0, 0)
    assert _raises(ValueError, b.add, "y", "HOLD", 100.0, 1)
    assert _raises(ValueError, b.add, "b1", "BUY", 100.0, 1)


# ---------------------------------------------------------------- Part 2
def test_part2_cancel(m):
    b = m.OrderBook()
    b.add("b1", "BUY", 100.0, 5)
    b.add("b2", "BUY", 99.0, 5)
    assert b.cancel("b1") is True and b.best_bid() == 99.0
    assert b.cancel("b1") is False and b.cancel("nope") is False


def test_part2_cancelled_order_never_trades(m):
    b = m.OrderBook()
    b.add("s1", "SELL", 100.0, 5)
    b.add("s2", "SELL", 101.0, 5)
    b.cancel("s1")
    assert b.add("b1", "BUY", 101.0, 5) == [m.Trade("b1", "s2", 101.0, 5)]


def test_part2_cannot_cancel_filled(m):
    b = m.OrderBook()
    b.add("s1", "SELL", 100.0, 5)
    b.add("b1", "BUY", 100.0, 5)
    assert b.cancel("s1") is False and b.cancel("b1") is False


def test_part2_depth(m):
    b = m.OrderBook()
    b.add("b1", "BUY", 100.0, 5)
    b.add("b2", "BUY", 100.0, 3)
    b.add("b3", "BUY", 99.0, 2)
    b.add("b4", "BUY", 98.0, 1)
    b.add("s1", "SELL", 101.0, 4)
    b.add("s2", "SELL", 102.0, 6)
    assert b.depth("BUY", 2) == [(100.0, 8), (99.0, 2)]
    assert b.depth("SELL", 5) == [(101.0, 4), (102.0, 6)]
    b.add("s3", "SELL", 100.0, 6)   # fills b1 (5) and 1 of b2
    assert b.depth("BUY", 1) == [(100.0, 2)]
    b.cancel("b3")
    assert b.depth("BUY", 3) == [(100.0, 2), (98.0, 1)]


# ---------------------------------------------------------------- Part 3
def test_part3_modify_down_keeps_priority(m):
    b = m.OrderBook()
    b.add("s1", "SELL", 100.0, 5)
    b.add("s2", "SELL", 100.0, 5)
    assert b.modify("s1", 2) is True
    assert b.add("b", "BUY", 100.0, 3) == [m.Trade("b", "s1", 100.0, 2), m.Trade("b", "s2", 100.0, 1)]


def test_part3_modify_up_loses_priority(m):
    b = m.OrderBook()
    b.add("s1", "SELL", 100.0, 5)
    b.add("s2", "SELL", 100.0, 5)
    b.modify("s1", 8)
    assert b.depth("SELL", 1) == [(100.0, 13)]
    assert b.add("b", "BUY", 100.0, 6) == [m.Trade("b", "s2", 100.0, 5), m.Trade("b", "s1", 100.0, 1)]


def test_part3_modify_zero_and_unknown(m):
    b = m.OrderBook()
    b.add("s1", "SELL", 100.0, 5)
    assert b.modify("s1", 0) is True and b.best_ask() is None
    assert b.modify("s1", 3) is False and b.modify("nope", 3) is False


def test_part3_performance_and_invariants(m):
    rng = random.Random(0)
    b, live, n_ops = m.OrderBook(), [], 100_000
    t0 = time.perf_counter()
    for i in range(n_ops):
        r = rng.random()
        if r < 0.25 and live:
            b.cancel(live.pop(rng.randrange(len(live))))
        elif r < 0.35 and live:
            b.modify(live[rng.randrange(len(live))], rng.randint(1, 20))
        else:
            side = "BUY" if rng.random() < 0.5 else "SELL"
            px = 100.0 + rng.randint(-50, 50) * 0.5
            oid = f"o{i}"
            b.add(oid, side, px, rng.randint(1, 20))
            live.append(oid)
        if i % 1000 == 0:
            bid, ask = b.best_bid(), b.best_ask()
            assert bid is None or ask is None or bid < ask, f"book crossed: {bid} >= {ask}"
    elapsed = time.perf_counter() - t0
    assert elapsed < 5.0, f"{n_ops:,} ops took {elapsed:.1f}s"
