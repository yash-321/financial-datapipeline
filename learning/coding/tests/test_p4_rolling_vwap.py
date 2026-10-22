"""Tests for P4. Each test takes the module under test as `m`."""

import math
import random
import time


def close(a, b):
    return (a is None and b is None) or (a is not None and b is not None and math.isclose(a, b, rel_tol=1e-9))


def brute_vwap(trades, symbol, now, window):
    sel = [(p, q) for ts, s, p, q in trades if s == symbol and now - window < ts <= now]
    qty = sum(q for _, q in sel)
    return sum(p * q for p, q in sel) / qty if qty else None


# ---------------------------------------------------------------- Part 1
def test_part1_basic_window(m):
    w = m.RollingVWAP(1000)
    w.on_trade(0, "A", 10.0, 100)
    w.on_trade(500, "A", 12.0, 100)
    assert close(w.vwap("A", 500), 11.0)
    assert close(w.vwap("A", 1000), 12.0)       # ts=0 is not > 1000-1000
    assert w.vwap("A", 1500) is None            # ts=500 is not > 1500-1000
    assert w.vwap("ZZZ", 1500) is None


def test_part1_boundaries(m):
    w = m.RollingVWAP(100)
    w.on_trade(100, "A", 1.0, 1)
    w.on_trade(200, "A", 3.0, 1)
    assert close(w.vwap("A", 200), 3.0)          # 100 excluded, 200 included


def test_part1_quantity_weighted_and_per_symbol(m):
    w = m.RollingVWAP(10_000)
    w.on_trade(1, "A", 10.0, 1)
    w.on_trade(2, "B", 99.0, 50)
    w.on_trade(3, "A", 20.0, 3)
    assert close(w.vwap("A", 3), 17.5) and close(w.vwap("B", 3), 99.0)


def test_part1_matches_brute_force_and_is_fast(m):
    rng = random.Random(1)
    window, w, trades, ts = 5_000, m.RollingVWAP(5_000), [], 0
    t0 = time.perf_counter()
    for _ in range(200_000):
        ts += rng.randint(0, 40)
        t = (ts, rng.choice("ABC"), round(rng.uniform(90, 110), 2), rng.randint(1, 500))
        trades.append(t)
        w.on_trade(*t)
        if rng.random() < 0.05:
            w.vwap(rng.choice("ABC"), ts)
    elapsed = time.perf_counter() - t0
    for sym in "ABC":
        assert close(round(w.vwap(sym, ts), 6), round(brute_vwap(trades[-2000:], sym, ts, window), 6))
    assert elapsed < 5.0, f"too slow: {elapsed:.1f}s (re-scanning history?)"


# ---------------------------------------------------------------- Part 2
def test_part2_replays_ignored(m):
    w = m.RollingVWAP(1000)
    assert w.on_trade(0, "A", 10.0, 1, trade_id="t1") is True
    assert w.on_trade(0, "A", 10.0, 1, trade_id="t1") is False
    assert w.on_trade(10, "A", 20.0, 1, trade_id="t2") is True
    assert close(w.vwap("A", 10), 15.0)


def test_part2_trades_without_id_always_accepted(m):
    w = m.RollingVWAP(1000)
    assert w.on_trade(0, "A", 10.0, 1) is True and w.on_trade(0, "A", 10.0, 1) is True
    assert close(w.vwap("A", 0), 10.0)


def test_part2_memory_bounded(m):
    w = m.RollingVWAP(1000)
    for i in range(10_000):
        w.on_trade(i * 10, "A", 10.0, 1, trade_id=f"t{i}")
    assert w.tracked_ids() <= 250, f"remembering {w.tracked_ids()} ids; only ~100 are within the window"


# ---------------------------------------------------------------- Part 3
def test_part3_late_trade_counted(m):
    w = m.RollingVWAP(1000, max_lateness_ms=200)
    assert w.on_trade(1000, "A", 10.0, 1) is True
    assert w.on_trade(900, "A", 20.0, 1) is True
    assert close(w.vwap("A", 1000), 15.0)


def test_part3_too_late_rejected(m):
    w = m.RollingVWAP(1000, max_lateness_ms=200)
    w.on_trade(1000, "A", 10.0, 1)
    assert w.on_trade(700, "A", 30.0, 1) is False and w.rejected_late == 1
    assert w.on_trade(800, "A", 30.0, 1) is True and w.rejected_late == 1   # exactly at the limit is fine


def test_part3_late_but_outside_window(m):
    w = m.RollingVWAP(100, max_lateness_ms=500)
    w.on_trade(1000, "A", 10.0, 1)
    assert close(w.vwap("A", 1000), 10.0)
    assert w.on_trade(850, "A", 50.0, 1) is True
    assert close(w.vwap("A", 1000), 10.0)


def test_part3_matches_brute_force(m):
    rng = random.Random(2)
    window, late = 2_000, 300
    w = m.RollingVWAP(window, max_lateness_ms=late)
    accepted, max_ts, clock = [], None, 0
    for i in range(30_000):
        clock += rng.randint(0, 30)
        ts = clock - rng.randint(0, 400)            # some beyond the lateness limit
        t = (ts, rng.choice("AB"), round(rng.uniform(90, 110), 2), rng.randint(1, 100))
        ok = w.on_trade(*t, trade_id=f"t{i}")
        assert ok == (max_ts is None or ts >= max_ts - late), f"accept/reject wrong for ts={ts}, max_ts={max_ts}"
        if ok:
            accepted.append(t)
            max_ts = ts if max_ts is None else max(max_ts, ts)
        if i % 500 == 0:
            for sym in "AB":
                assert close(w.vwap(sym, max_ts), brute_vwap(accepted, sym, max_ts, window))
    assert w.rejected_late == 30_000 - len(accepted)
