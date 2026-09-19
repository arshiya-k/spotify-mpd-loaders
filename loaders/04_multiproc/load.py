"""Loader 04: multiprocessing. N worker processes each parse + COPY slices on their
own Postgres connection. Dimension rows go to staging (no coordination needed);
the parent collapses them with GROUP BY once all workers finish.

Why processes, not threads: parsing is CPU-bound Python, and the GIL means threads
would serialize on one core. Processes get real parallelism at the cost of sharing
nothing -- hence staging instead of loader 03's in-memory dedup.
"""
import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import psycopg

from common import db
from common.cli import slices_from_env
from common.copy import copy_slice_to_staging
from common.parse import parse_slice

WORKERS = int(os.environ.get("MPD_WORKERS") or os.cpu_count() or 4)

# Module-level global, set by the initializer. Each worker PROCESS gets its own copy
# of this module, so this is one connection per worker, not one shared connection.
_conn: psycopg.Connection | None = None


def _init_worker() -> None:
    global _conn
    _conn = db.connect()


def _load_one(path: Path) -> int:
    """Runs inside a worker. Returns the entry count so the parent can report progress."""
    rows = parse_slice(path)
    copy_slice_to_staging(_conn, rows)
    _conn.commit()
    return len(rows.entries)


def main() -> None:
    files = slices_from_env()

    with db.connect() as conn:
        db.create_staging(conn)

    total = 0
    t0 = time.perf_counter()
    with ProcessPoolExecutor(max_workers=WORKERS, initializer=_init_worker) as pool:
        futures = {pool.submit(_load_one, f): f for f in files}
        for i, fut in enumerate(as_completed(futures), 1):
            total += fut.result()          # re-raises any worker exception here
            if i % 50 == 0:
                print(f"  {i} slices, {total:,} entries")

    t_parallel = time.perf_counter() - t0

    t0 = time.perf_counter()
    with db.connect() as conn:
        db.finalize_staging(conn)
    print(f"  workers={WORKERS}  parallel copy {t_parallel:.1f}s  finalize {time.perf_counter() - t0:.1f}s")


if __name__ == "__main__":
    main()
