"""Tests for P3. Each test takes the module under test as `m`."""

DEPS = {"report": ["clean", "prices"], "clean": ["load"], "prices": ["load"], "load": [], "other": []}


def _cycle_error(m, deps):
    try:
        m.run_order(deps)
    except m.CycleError as e:
        return e
    raise AssertionError("expected CycleError")


def _is_valid_order(deps, order):
    pos = {j: i for i, j in enumerate(order)}
    return all(pos[d] < pos[j] for j, ds in deps.items() for d in ds)


# ---------------------------------------------------------------- Part 1
def test_part1_simple_chain(m):
    assert m.run_order({"report": ["clean", "load"], "clean": ["load"], "load": []}) == ["load", "clean", "report"]


def test_part1_jobs_only_listed_as_deps(m):
    assert m.run_order({"b": ["a"]}) == ["a", "b"]


def test_part1_alphabetical_tie_break(m):
    assert m.run_order({"c": [], "a": [], "b": []}) == ["a", "b", "c"]
    assert m.run_order({"z": ["a"], "b": ["a"]}) == ["a", "b", "z"]
    order = m.run_order(DEPS)
    assert order == ["load", "clean", "other", "prices", "report"] and _is_valid_order(DEPS, order)


def test_part1_duplicates_and_empty(m):
    assert m.run_order({"b": ["a", "a"]}) == ["a", "b"]
    assert m.run_order({}) == []


def test_part1_cycle_reported(m):
    deps = {"a": ["b"], "b": ["c"], "c": ["a"], "d": [], "e": ["a"]}
    cyc = _cycle_error(m, deps).cycle
    assert cyc[0] == cyc[-1] and len(cyc) == 4 and set(cyc) == {"a", "b", "c"}
    assert all(cyc[i + 1] in deps[cyc[i]] for i in range(len(cyc) - 1)), f"not a real cycle: {cyc}"


def test_part1_self_loop(m):
    assert _cycle_error(m, {"a": ["a"]}).cycle == ["a", "a"]


def test_part1_scales(m):
    n = 50_000
    deps = {f"j{i:05d}": [f"j{i - 1:05d}"] + ([f"j{i // 2:05d}"] if i > 2 else []) for i in range(1, n)}
    order = m.run_order(deps)
    assert len(order) == n and _is_valid_order(deps, order)


# ---------------------------------------------------------------- Part 2
def test_part2_levels(m):
    assert m.levels(DEPS) == [["load", "other"], ["clean", "prices"], ["report"]]


def test_part2_levels_longest_chain_decides(m):
    assert m.levels({"c": ["a", "b"], "b": ["a"]}) == [["a"], ["b"], ["c"]]


def test_part2_levels_cycle(m):
    try:
        m.levels({"a": ["b"], "b": ["a"]})
    except m.CycleError:
        return
    raise AssertionError("expected CycleError")


# ---------------------------------------------------------------- Part 3
def test_part3_blocked(m):
    assert m.blocked(DEPS, {"load"}) == {"clean", "prices", "report"}
    assert m.blocked(DEPS, {"prices"}) == {"report"}
    assert m.blocked(DEPS, {"report", "other"}) == set()
    assert m.blocked(DEPS, set()) == set()


def test_part3_earliest_finish(m):
    durations = {"load": 3, "clean": 2, "prices": 5, "report": 1, "other": 4}
    assert m.earliest_finish(DEPS, durations) == (9, ["load", "prices", "report"])


def test_part3_earliest_finish_independent_job_dominates(m):
    durations = {"load": 1, "clean": 1, "prices": 1, "report": 1, "other": 10}
    assert m.earliest_finish(DEPS, durations) == (10, ["other"])


def test_part3_earliest_finish_empty(m):
    assert m.earliest_finish({}, {}) == (0, [])
