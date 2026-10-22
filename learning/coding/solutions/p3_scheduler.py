"""Reference solution: P3 job dependency scheduler (all three parts)."""

import heapq
from collections import defaultdict


class CycleError(Exception):
    def __init__(self, cycle: list[str]) -> None:
        super().__init__(" -> ".join(cycle))
        self.cycle = cycle


def _graph(deps: dict[str, list[str]]):
    """All jobs, plus job -> dependents and job -> number of unmet deps."""
    jobs = set(deps) | {d for ds in deps.values() for d in ds}
    dependents: dict[str, set[str]] = defaultdict(set)
    indegree = {j: 0 for j in jobs}
    for job, ds in deps.items():
        for d in set(ds):
            dependents[d].add(job)
            indegree[job] += 1
    return jobs, dependents, indegree


def _find_cycle(deps: dict[str, list[str]], nodes: set[str]) -> list[str]:
    """DFS over the jobs Kahn's algorithm couldn't schedule. Every one of them is on or behind a cycle."""
    state: dict[str, int] = {}          # 1 = on stack, 2 = done
    stack: list[str] = []

    def dfs(n: str):
        state[n] = 1
        stack.append(n)
        for d in sorted(deps.get(n, [])):
            if d not in nodes:
                continue
            if state.get(d) == 1:
                return stack[stack.index(d):] + [d]
            if d not in state:
                found = dfs(d)
                if found:
                    return found
        stack.pop()
        state[n] = 2
        return None

    for n in sorted(nodes):
        if n not in state:
            found = dfs(n)
            if found:
                return found
    raise AssertionError("unreachable: leftover nodes must contain a cycle")


# ---------------------------------------------------------------- Part 1
def run_order(deps: dict[str, list[str]]) -> list[str]:
    jobs, dependents, indegree = _graph(deps)
    ready = [j for j in jobs if indegree[j] == 0]
    heapq.heapify(ready)                 # alphabetical tie-break
    order = []
    while ready:
        j = heapq.heappop(ready)
        order.append(j)
        for k in dependents[j]:
            indegree[k] -= 1
            if indegree[k] == 0:
                heapq.heappush(ready, k)
    if len(order) < len(jobs):
        raise CycleError(_find_cycle(deps, jobs - set(order)))
    return order


# ---------------------------------------------------------------- Part 2
def levels(deps: dict[str, list[str]]) -> list[list[str]]:
    jobs, dependents, indegree = _graph(deps)
    batch = sorted(j for j in jobs if indegree[j] == 0)
    out, done = [], 0
    while batch:
        out.append(batch)
        done += len(batch)
        nxt = []
        for j in batch:
            for k in dependents[j]:
                indegree[k] -= 1
                if indegree[k] == 0:
                    nxt.append(k)
        batch = sorted(nxt)
    if done < len(jobs):
        run_order(deps)                  # raises CycleError with the cycle
    return out


# ---------------------------------------------------------------- Part 3
def blocked(deps: dict[str, list[str]], failed: set[str]) -> set[str]:
    _, dependents, _ = _graph(deps)
    seen: set[str] = set()
    todo = list(failed)
    while todo:
        j = todo.pop()
        for k in dependents[j]:
            if k not in seen:
                seen.add(k)
                todo.append(k)
    return seen - failed


def earliest_finish(deps: dict[str, list[str]], durations: dict[str, int]) -> tuple[int, list[str]]:
    finish: dict[str, int] = {}
    via: dict[str, str] = {}
    for j in run_order(deps):
        start = 0
        for d in sorted(set(deps.get(j, []))):
            if finish[d] > start:
                start, via[j] = finish[d], d
        finish[j] = start + durations[j]
    if not finish:
        return 0, []
    end = max(sorted(finish), key=lambda j: finish[j])
    path = [end]
    while path[-1] in via:
        path.append(via[path[-1]])
    return finish[end], path[::-1]
