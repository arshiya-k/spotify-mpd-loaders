"""Run one or more loaders through reset -> load -> constraints -> validate, and record timings.

Usage:
    python benchmark.py --slices 10 03_vectorized
    python benchmark.py --slices 100 02_sequential 03_vectorized 04_multiproc
    python benchmark.py 03_vectorized               # full dataset

Each loader is a directory under loaders/ containing a `run.sh`. The contract:
    - env: DATABASE_URL, MPD_DATA_DIR, MPD_SLICES (empty string = all)
    - exit 0 on success; anything else fails the benchmark
The harness does everything else (schema reset, constraints, validation, timing).
"""
import argparse
import csv
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from common import db
from common.config import DATA_DIR, DATABASE_URL
from common.expected import expected_counts

REPO = Path(__file__).resolve().parent
LOADERS_DIR = REPO / "loaders"
RESULTS_CSV = REPO / "results" / "results.csv"


def run_loader(name: str, n_slices: int | None) -> float:
    """Shell out to loaders/<name>/run.sh and return wall seconds."""
    script = LOADERS_DIR / name / "run.sh"
    if not script.exists():
        sys.exit(f"no such loader: {script}")

    env = {
        **os.environ,
        "DATABASE_URL": DATABASE_URL,
        "MPD_DATA_DIR": str(DATA_DIR),
        "MPD_SLICES": str(n_slices or ""),
    }
    t0 = time.perf_counter()
    subprocess.run(["bash", str(script)], cwd=REPO, env=env, check=True)
    return time.perf_counter() - t0


def validate(actual: dict[str, int], expected: dict[str, int]) -> list[str]:
    return [
        f"{t}: expected {expected[t]:,}, got {actual[t]:,}"
        for t in expected
        if actual[t] != expected[t]
    ]


def benchmark(name: str, n_slices: int | None, expected: dict[str, int]) -> dict:
    print(f"\n=== {name}  ({n_slices or 'all'} slices) ===")

    with db.connect() as conn:
        db.reset_schema(conn)

    t_load = run_loader(name, n_slices)
    print(f"  load:        {t_load:8.1f}s")

    with db.connect() as conn:
        t0 = time.perf_counter()
        db.add_constraints(conn)
        t_constraints = time.perf_counter() - t0
        print(f"  constraints: {t_constraints:8.1f}s")
        counts = db.row_counts(conn)

    problems = validate(counts, expected)
    status = "OK" if not problems else "MISMATCH"
    print(f"  validate:    {status}")
    for p in problems:
        print(f"    ! {p}")

    return {
        "run_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "loader": name,
        "slices": n_slices or 1000,
        "load_s": round(t_load, 1),
        "constraints_s": round(t_constraints, 1),
        "total_s": round(t_load + t_constraints, 1),
        "rows_per_s": round(counts["playlist_tracks"] / t_load),
        "status": status,
        **counts,
    }


def append_result(row: dict) -> None:
    RESULTS_CSV.parent.mkdir(exist_ok=True)
    new_file = not RESULTS_CSV.exists()
    with RESULTS_CSV.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=row.keys())
        if new_file:
            w.writeheader()
        w.writerow(row)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("loaders", nargs="+", help="loader directory names under loaders/")
    ap.add_argument("--slices", type=int, default=None, help="first N slice files (default: all 1000)")
    ap.add_argument("--repeat", type=int, default=1, help="run each loader N times (report all runs)")
    args = ap.parse_args()

    print(f"computing expected counts for {args.slices or 'all'} slices...")
    expected = expected_counts(args.slices)

    for rep in range(args.repeat):
        for name in args.loaders:
            row = benchmark(name, args.slices, expected)
            row["rep"] = rep + 1
            append_result(row)

    print(f"\nresults appended to {RESULTS_CSV.relative_to(REPO)}")


if __name__ == "__main__":
    main()
