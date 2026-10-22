# P1: Identifier mapping (~45 min, plain Python, no pandas)

*This is your round-1 problem without pandas, with harder follow-ups. A coding round could plausibly build on it.*

We buy reference data from a vendor and need to map each vendor instrument to our internal symbol.
Both sides carry the same three industry identifier codes: `isin`, `cusip` and `sedol`.

```python
vendor_rows   = [{"vendor_id": "V1", "isin": "US0000000001", "cusip": "", "sedol": None}, ...]
internal_rows = [{"symbol": "AAA", "date": "2026-10-01", "isin": "US0000000001", "cusip": "", "sedol": ""}, ...]
```

* A code is **missing** if it's `None`, `""`, whitespace-only, or the key is absent.
* Codes compare **case-insensitively, after stripping whitespace**.
* Codes only match within the same field. An `isin` never matches a `cusip`.
* The internal table has one row per symbol per date, so a symbol usually appears many times.

Work in `p1_id_mapping.py`. Test with `python run.py p1 --part 1` (then `--part 2`, and so on).

## Part 1

Write `build_mapping(vendor_rows, internal_rows) -> list[tuple[str, str]]`.

Return every **distinct** `(vendor_id, symbol)` pair where the two rows share at least one populated code in the same
field, sorted ascending.

Say your time and space complexity. A nested loop over both tables works, but can you do better?

---
> ⛔ **Stop.** Get Part 1 passing before reading on. In the interview, the next part only appears once you're done.
---

## Part 2

Sharing one code isn't enough. If a vendor row and an internal row both populate a field and **disagree** on it,
the pair is a contradiction and must not match, even if another field agrees.

Write `build_mapping_strict(vendor_rows, internal_rows) -> tuple[list[tuple[str, str]], dict[str, list[str]]]`:

* A (vendor row, internal row) pair matches if **at least one** shared populated field agrees and **no** shared
  populated field disagrees.
* `matches` holds sorted `(vendor_id, symbol)` pairs for vendors that match **exactly one** symbol.
* `rejected` maps every other vendor_id to the sorted list of symbols it matched. An empty list means unmatched; two or
  more means ambiguous.

---
> ⛔ **Stop.**
---

## Part 3

Symbols change over time: a company renames, or a corporate action changes a code. We need the mapping **with
dates**.

Write `build_mapping_ranges(vendor_rows, internal_rows) -> list[tuple[str, str, str, str]]` returning
`(vendor_id, symbol, start_date, end_date)` with inclusive dates:

* Apply the Part 2 rule **per internal row**, so a vendor maps to a symbol *on that row's date*.
* If on some date a vendor matches more than one symbol, that date is ambiguous. **Exclude it.**
* Collapse each vendor-symbol's dates into ranges. Two dates are consecutive if they're adjacent in the sorted list
  of **all distinct dates in the internal table**, so weekends and holidays don't break a range.
* Sort by `(vendor_id, start_date, symbol)`.

## Discussion questions (expect these verbally)

* The internal table has 50M rows and the vendor table has 2M. What changes? Think about indexing on `(field, value)`,
  memory, and sorting or hashing in SQL.
* Product wants ambiguity resolved rather than rejected, for example by field priority ISIN > CUSIP > SEDOL. How
  would you change Part 2?
* How would you test this in production? Think about row-count reconciliation and the share of vendors mapped day
  over day.
