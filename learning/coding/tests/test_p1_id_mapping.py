"""Tests for P1. Each test takes the module under test as `m`."""

D1, D2, D3, D4, D5 = "2026-10-01", "2026-10-02", "2026-10-05", "2026-10-06", "2026-10-07"  # Thu, Fri, Mon, Tue, Wed


def V(vid, **codes):
    return {"vendor_id": vid, **codes}


def I(sym, date=D1, **codes):
    return {"symbol": sym, "date": date, **codes}


# ---------------------------------------------------------------- Part 1
def test_part1_basic_match(m):
    assert m.build_mapping([V("V1", isin="US01", cusip="", sedol=None)],
                           [I("AAA", isin="US01", cusip="", sedol="")]) == [("V1", "AAA")]


def test_part1_empty_strings_never_match(m):
    vendor = [V("V1", isin="", cusip="", sedol=""), V("V2", isin="  ", cusip=None, sedol="")]
    internal = [I("AAA", isin="", cusip="", sedol=""), I("BBB", isin=None, cusip="  ", sedol=None)]
    assert m.build_mapping(vendor, internal) == []


def test_part1_normalises_case_and_whitespace(m):
    assert m.build_mapping([V("V1", isin=" us01 ")], [I("AAA", isin="US01")]) == [("V1", "AAA")]


def test_part1_same_value_in_different_field_does_not_match(m):
    assert m.build_mapping([V("V1", cusip="123456789")], [I("AAA", sedol="123456789")]) == []


def test_part1_absent_keys_are_missing(m):
    assert m.build_mapping([V("V1", isin="X1")], [I("AAA", sedol="S9"), I("BBB", isin="X1")]) == [("V1", "BBB")]


def test_part1_distinct_and_sorted(m):
    vendor = [V("V2", isin="X2", cusip="C2"), V("V1", sedol="S1")]
    internal = [I("BBB", D1, isin="X2", cusip="C2"), I("BBB", D2, isin="X2", cusip="C2"), I("AAA", sedol="S1")]
    assert m.build_mapping(vendor, internal) == [("V1", "AAA"), ("V2", "BBB")]


def test_part1_permissive_multiple_symbols(m):
    vendor = [V("V1", isin="X1", cusip="C9")]
    internal = [I("AAA", isin="X1"), I("BBB", cusip="C9")]
    assert m.build_mapping(vendor, internal) == [("V1", "AAA"), ("V1", "BBB")]


def test_part1_scales(m):
    n = 20_000
    vendor = [V(f"V{i:05d}", isin=f"X{i}", cusip="", sedol="") for i in range(n)]
    internal = [I(f"S{i:05d}", isin=f"X{i}", cusip="", sedol="") for i in range(n)]
    assert len(m.build_mapping(vendor, internal)) == n   # a nested loop (4e8 comparisons) will crawl


# ---------------------------------------------------------------- Part 2
def test_part2_contradiction_blocks_match(m):
    matches, rejected = m.build_mapping_strict([V("V1", isin="X1", cusip="C1")], [I("AAA", isin="X1", cusip="C2")])
    assert matches == [] and rejected == {"V1": []}


def test_part2_missing_side_is_not_a_contradiction(m):
    matches, rejected = m.build_mapping_strict([V("V1", isin="X1", cusip="")], [I("AAA", isin="X1", cusip="C1")])
    assert matches == [("V1", "AAA")] and rejected == {}


def test_part2_ambiguous(m):
    matches, rejected = m.build_mapping_strict([V("V1", isin="X1")], [I("BBB", isin="X1"), I("AAA", isin="x1")])
    assert matches == [] and rejected == {"V1": ["AAA", "BBB"]}


def test_part2_mixed(m):
    vendor = [V("V1", isin="X1"), V("V2", sedol="S2"), V("V3", isin="", cusip="", sedol="")]
    internal = [I("AAA", D1, isin="X1"), I("AAA", D2, isin="X1"), I("BBB", sedol="S2", isin="X9")]
    matches, rejected = m.build_mapping_strict(vendor, internal)
    assert matches == [("V1", "AAA"), ("V2", "BBB")] and rejected == {"V3": []}


# ---------------------------------------------------------------- Part 3
def test_part3_rename_splits_ranges(m):
    internal = [I("AAA", d, isin="X1") for d in (D1, D2, D3)] + [I("AAB", d, isin="X1") for d in (D4, D5)]
    assert m.build_mapping_ranges([V("V1", isin="X1")], internal) == [("V1", "AAA", D1, D3), ("V1", "AAB", D4, D5)]


def test_part3_missing_date_is_a_gap(m):
    internal = [I("AAA", d, isin="X1") for d in (D1, D2, D4, D5)] + [I("ZZZ", D3, isin="X9")]
    assert m.build_mapping_ranges([V("V1", isin="X1")], internal) == [("V1", "AAA", D1, D2), ("V1", "AAA", D4, D5)]


def test_part3_weekend_is_not_a_gap(m):
    internal = [I("AAA", D2, isin="X1"), I("AAA", D3, isin="X1")]
    assert m.build_mapping_ranges([V("V1", isin="X1")], internal) == [("V1", "AAA", D2, D3)]


def test_part3_ambiguous_dates_excluded(m):
    internal = [I("AAA", d, isin="X1") for d in (D1, D2, D3)] + [I("BBB", D2, isin="X1")]
    assert m.build_mapping_ranges([V("V1", isin="X1")], internal) == [("V1", "AAA", D1, D1), ("V1", "AAA", D3, D3)]


def test_part3_code_change_over_time(m):
    internal = [I("AAA", D1, isin="X1", cusip="C1"), I("AAA", D2, isin="X1", cusip="C1"),
                I("AAA", D3, isin="X1", cusip="C2")]
    assert m.build_mapping_ranges([V("V1", isin="X1", cusip="C1")], internal) == [("V1", "AAA", D1, D2)]


def test_part3_sorted_across_vendors(m):
    internal = [I("AAA", D1, isin="X1"), I("BBB", D1, isin="X2"), I("BBB", D2, isin="X2")]
    vendor = [V("V2", isin="X1"), V("V1", isin="X2")]
    assert m.build_mapping_ranges(vendor, internal) == [("V1", "BBB", D1, D2), ("V2", "AAA", D1, D1)]
