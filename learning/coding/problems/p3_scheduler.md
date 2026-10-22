# P3: Pipeline job scheduler (~40 min)

A data platform runs jobs that depend on other jobs. Dependencies are given as
`deps: dict[str, list[str]]`, where `deps[job]` lists the jobs that must finish **before** `job`. A job may
appear only inside dependency lists and never as a key. It's still a job.

```python
deps = {"report": ["clean", "prices"], "clean": ["load"], "prices": ["load"], "load": []}
```

Work in `p3_scheduler.py` (a `CycleError` exception is provided). Test with `python run.py p3 --part 1`.

## Part 1

`run_order(deps) -> list[str]` returns a valid execution order. When several jobs are ready at once, pick the
**alphabetically smallest** first, so the output is deterministic.

If the dependencies contain a cycle, raise `CycleError(cycle)` where `cycle` is a list like
`["a", "b", "c", "a"]`. Each job depends on the next one, and the list starts and ends with the same job.

---
> ⛔ **Stop.** Get Part 1 passing before reading on.
---

## Part 2

`levels(deps) -> list[list[str]]` groups jobs into batches that can run **in parallel**. Batch 0 holds jobs with no
dependencies. Batch *k* holds jobs whose dependencies all appear in earlier batches. Sort each batch alphabetically.
Raise `CycleError` on a cycle.

---
> ⛔ **Stop.**
---

## Part 3

* `blocked(deps, failed: set[str]) -> set[str]` returns every job that can't run because something upstream
  failed, directly or transitively. Exclude the failed jobs themselves.
* `earliest_finish(deps, durations: dict[str, int]) -> tuple[int, list[str]]` assumes unlimited workers. Return
  the time at which everything is done and **the critical path** that determines it, ordered from first job to last.

## Discussion questions

* What's the complexity of Kahn's algorithm, and why does the alphabetical tie-break add a log factor?
* Now there are only `k` workers. Does `levels` still give the fastest schedule? (No. Why not?)
* How would you re-run only what's needed after one job's input data changes?
