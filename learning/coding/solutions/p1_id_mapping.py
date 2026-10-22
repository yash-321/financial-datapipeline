"""Reference solution: P1 identifier mapping (all three parts)."""

from collections import defaultdict
from typing import Optional

CODE_FIELDS = ("isin", "cusip", "sedol")


def norm(value: Optional[str]) -> Optional[str]:
    """Missing (None, "", whitespace) -> None; otherwise stripped and upper-cased."""
    if value is None:
        return None
    value = value.strip().upper()
    return value or None


def codes(row: dict) -> dict[str, str]:
    """Only the populated, normalised codes of a row."""
    out = {}
    for field in CODE_FIELDS:
        v = norm(row.get(field))
        if v is not None:
            out[field] = v
    return out


# ---------------------------------------------------------------- Part 1
def build_mapping(vendor_rows: list[dict], internal_rows: list[dict]) -> list[tuple[str, str]]:
    index: dict[tuple[str, str], set[str]] = defaultdict(set)   # (field, value) -> symbols
    for row in internal_rows:
        for field, value in codes(row).items():
            index[(field, value)].add(row["symbol"])

    pairs = set()
    for row in vendor_rows:
        for field, value in codes(row).items():
            for symbol in index.get((field, value), ()):
                pairs.add((row["vendor_id"], symbol))
    return sorted(pairs)


# ---------------------------------------------------------------- Part 2
def rows_match(a: dict[str, str], b: dict[str, str]) -> bool:
    """At least one shared field agrees, and no shared field disagrees."""
    shared = a.keys() & b.keys()
    return bool(shared) and all(a[f] == b[f] for f in shared)


def _candidate_rows(vendor_codes: dict[str, str], index, internal_codes):
    """Internal row positions sharing any code with the vendor row (cheap pre-filter)."""
    hits = set()
    for field, value in vendor_codes.items():
        hits |= index.get((field, value), set())
    return [i for i in hits if rows_match(vendor_codes, internal_codes[i])]


def _index_rows(internal_rows):
    internal_codes = [codes(r) for r in internal_rows]
    index: dict[tuple[str, str], set[int]] = defaultdict(set)   # (field, value) -> row positions
    for i, c in enumerate(internal_codes):
        for field, value in c.items():
            index[(field, value)].add(i)
    return internal_codes, index


def build_mapping_strict(vendor_rows: list[dict], internal_rows: list[dict]):
    internal_codes, index = _index_rows(internal_rows)
    symbols_by_vendor: dict[str, set[str]] = defaultdict(set)
    for row in vendor_rows:
        vid = row["vendor_id"]
        symbols_by_vendor[vid]  # make sure unmatched vendors appear
        for i in _candidate_rows(codes(row), index, internal_codes):
            symbols_by_vendor[vid].add(internal_rows[i]["symbol"])

    matches, rejected = [], {}
    for vid, syms in symbols_by_vendor.items():
        if len(syms) == 1:
            matches.append((vid, next(iter(syms))))
        else:
            rejected[vid] = sorted(syms)
    return sorted(matches), rejected


# ---------------------------------------------------------------- Part 3
def build_mapping_ranges(vendor_rows: list[dict], internal_rows: list[dict]) -> list[tuple[str, str, str, str]]:
    internal_codes, index = _index_rows(internal_rows)
    all_dates = sorted({r["date"] for r in internal_rows})
    pos = {d: i for i, d in enumerate(all_dates)}

    # vendor -> date -> symbols matched on that date
    by_date: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    for row in vendor_rows:
        for i in _candidate_rows(codes(row), index, internal_codes):
            r = internal_rows[i]
            by_date[row["vendor_id"]][r["date"]].add(r["symbol"])

    out = []
    for vid, dates in by_date.items():
        # keep unambiguous dates only, then collapse consecutive (in all_dates order) runs per symbol
        per_symbol: dict[str, list[str]] = defaultdict(list)
        for d, syms in dates.items():
            if len(syms) == 1:
                per_symbol[next(iter(syms))].append(d)
        for sym, ds in per_symbol.items():
            ds.sort()
            start = prev = ds[0]
            for d in ds[1:]:
                if pos[d] != pos[prev] + 1:
                    out.append((vid, sym, start, prev))
                    start = d
                prev = d
            out.append((vid, sym, start, prev))
    return sorted(out, key=lambda t: (t[0], t[2], t[1]))
