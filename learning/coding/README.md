# Coding round prep (1 hour)

## What to expect

This is a best guess. It's based on the recruiter's wording, the format of your first round, and how Jane Street
describes its interviews publicly. Confirm the details with the recruiter (questions below).

* **One problem with several parts that grow.** You solve Part 1, then the interviewer adds a requirement, then
  another. Most candidates don't finish every part, and that's expected.
* **Practical rather than tricky.** You're more likely to build a small working component (a matcher, a mapper, a
  scheduler, a stream processor) than to recall an obscure algorithm. You still need the core data structures:
  dict, set, heap, deque, bisect and sorting.
* **Collaborative.** The interviewer answers questions and gives hints. How you use a hint counts.
* **What's scored:** correct working code, a clean design that *survives the next part*, edge cases and testing,
  complexity awareness, and clear communication.
* **For a data engineer role:** expect plain-Python data processing (joining or mapping records, dedup, windows,
  intervals, DAGs). It may follow on from your round-1 problem. Don't assume pandas is allowed.

### Ask the recruiter (this is normal and expected)

* Which language? Can I choose Python?
* What's the environment: a shared editor, or a notebook? Can I run code?
* Is pandas or SQL in scope, or plain language only?
* Is it general software engineering, or data-focused like round 1?

## Round-1 debrief: one point worth knowing cold

Your mapping problem had empty-string codes. In pandas, **`merge` matches `""` to `""`, and it also matches NaN to
NaN.** That's unlike SQL, where `NULL = NULL` is never true. Merging on a code column with blanks quietly creates a
cross product of every blank row on both sides. The fix is to normalise (strip, upper-case, `""` → NaN) and then
**drop rows with a missing key before each merge**, one code at a time. If they ask you to revisit that problem,
leading with this shows you understood the real trap. P1 below is that problem in plain Python, with harder parts.

## The practice set

| Problem | Core skills | Time |
|---|---|---|
| [P1 identifier mapping](problems/p1_id_mapping.md) | hashing and indexing, normalisation, ambiguity rules, date ranges | ~45 min |
| [P2 order book](problems/p2_order_book.md) | heaps, lazy deletion, price-time priority, invariants | ~50 min |
| [P3 job scheduler](problems/p3_scheduler.md) | graphs, topological sort, cycle detection, critical path | ~40 min |
| [P4 rolling VWAP](problems/p4_rolling_vwap.md) | sliding windows, deque/bisect, dedup with bounded memory, late data | ~45 min |

Read one part of the `.md` at a time, code in the matching `problems/*.py`, and test as you go:

```bash
conda activate datapipeline && cd learning/coding
python run.py p1 --part 1        # then --part 2, --part 3
python run.py p1 --solution      # check the reference solution passes the same tests
```

Only look at `solutions/` after a timed attempt. Each `.md` ends with discussion questions. Answer them out
loud, because interviewers often spend the last 10 minutes on them.

## Habits that score in a coding round

1. **Restate the problem and work one example by hand** before coding. Ask about input sizes and edge cases.
2. **Get a simple, correct version first**, say its complexity, then improve it. A working O(n²) beats an
   unfinished O(n log n).
3. **Structure the code so it can be extended.** Use small helpers (`norm()`, `codes()`, `_top()`) so Part 3
   becomes a change, not a rewrite.
4. **Test while you go.** Write two or three `assert`s per part, including the empty input and a tie.
5. **Narrate your decisions, not your keystrokes.** "I'm using a heap because I need the best price repeatedly."
6. **When stuck, say what you're stuck on.** A hint you use well scores better than five silent minutes.

## Python standard library to have at your fingertips

| Need | Tool |
|---|---|
| grouping or counting | `collections.defaultdict(list)`, `Counter`, `setdefault` |
| best item repeatedly | `heapq.heappush/heappop`, negate the key for a max-heap, `(key, seq, item)` for ties |
| sliding window or queue | `collections.deque` with `append` / `popleft` |
| sorted insert or search | `bisect.bisect_left/right`, `bisect.insort` |
| runs of equal keys | `itertools.groupby` (**the input must be sorted by that key**) |
| prefix sums, pairs | `itertools.accumulate`, `itertools.pairwise` (Python 3.10+) |
| records | `@dataclass`, `namedtuple` |
| memoisation | `functools.lru_cache` / `cache` |
| deterministic output | `sorted(..., key=lambda t: (t[0], -t[1]))` |

## Plan before the interview

When you reply to the recruiter, offer slots about **7-10 days out**. That leaves time for this plan.

1. P1 under a timer, then compare with the solution. (It's the closest to round 1.)
2. P2. Spend time on *why* lazy deletion works.
3. P3, plus the discussion questions out loud.
4. P4, then redo whichever problem felt worst, from scratch, without looking.
5. A live mock with a person or Claude as the interviewer, revealing one part at a time and asking follow-ups.
