"""Run the tests for one practice problem, part by part.

    python run.py p1              # your code in problems/, all parts
    python run.py p1 --part 1     # only Part 1 tests
    python run.py p1 --solution   # the reference solution in solutions/
"""

import argparse
import importlib
import sys
import time
import traceback
from pathlib import Path

MODULES = {"p1": "p1_id_mapping", "p2": "p2_order_book", "p3": "p3_scheduler", "p4": "p4_rolling_vwap"}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("problem", choices=MODULES)
    ap.add_argument("--part", type=int, help="run tests up to and including this part")
    ap.add_argument("--solution", action="store_true", help="test the reference solution instead")
    ap.add_argument("-v", "--verbose", action="store_true", help="print full tracebacks")
    args = ap.parse_args()

    sys.path.insert(0, str(Path(__file__).resolve().parent))
    name = MODULES[args.problem]
    impl = importlib.import_module(f"{'solutions' if args.solution else 'problems'}.{name}")
    tests = importlib.import_module(f"tests.test_{name}")

    cases = [(n, f) for n, f in vars(tests).items() if n.startswith("test_part") and callable(f)]
    if args.part:
        cases = [(n, f) for n, f in cases if int(n[len("test_part")]) <= args.part]

    passed = 0
    for n, f in cases:
        t0 = time.perf_counter()
        try:
            f(impl)
        except (NotImplementedError, AttributeError) as e:
            print(f"TODO  {n}  ({type(e).__name__}: {e})")
            continue
        except Exception as e:  # AssertionError and anything the code under test raised
            print(f"FAIL  {n}  ({type(e).__name__}: {e})")
            if args.verbose:
                traceback.print_exc()
            continue
        passed += 1
        print(f"ok    {n}  [{(time.perf_counter() - t0) * 1000:.0f} ms]")
    print(f"\n{passed}/{len(cases)} passed")
    return 0 if passed == len(cases) else 1


if __name__ == "__main__":
    sys.exit(main())
