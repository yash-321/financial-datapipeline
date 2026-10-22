"""P3: pipeline job scheduler. Read p3_scheduler.md one part at a time.

Add levels (Part 2), blocked and earliest_finish (Part 3) when you reach them.
"""


class CycleError(Exception):
    def __init__(self, cycle: list[str]) -> None:
        super().__init__(" -> ".join(cycle))
        self.cycle = cycle


def run_order(deps: dict[str, list[str]]) -> list[str]:
    raise NotImplementedError
