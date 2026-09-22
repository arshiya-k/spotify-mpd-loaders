"""Loader 04: multiprocessing. N worker processes each parse + COPY slices on their
own Postgres connection. Dimension rows go to staging (no coordination needed);
the parent merges them into the real tables after each chunk.

Why processes, not threads: parsing is CPU-bound Python, and the GIL means threads
would serialize on one core. Processes get real parallelism at the cost of sharing
nothing -- hence staging instead of loader 03's in-memory dedup.

Why chunks: staging absorbs every duplicate, so loading all 1000 slices before
collapsing would put ~8 GB of soon-to-be-discarded rows on disk. Merging after each
chunk bounds the working set, making peak disk independent of dataset size.

The merge is a COMBINER, not a final dedup -- it GROUP BYs each chunk into unindexed
*_merged tables and leaves cross-chunk duplicates for one final pass. Deduping
straight into the real tables instead would need primary keys during load, and that
measured 5.6x slower (9.0s vs 1.6s on the same data) because it replaces one bulk
index build with per-row index maintenance.
"""
import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from itertools import islice
from pathlib import Path

import psycopg

from common import db
from common.cli import slices_from_env
from common.copy import copy_slice_to_staging
from common.parse import parse_slice

WORKERS = int(os.environ.get("MPD_WORKERS") or os.cpu_count() or 4)
CHUNK = int(os.environ.get("MPD_CHUNK") or 100)      # slices per merge

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


def chunks(seq, size):
    it = iter(seq)
    while batch := list(islice(it, size)):
        yield batch


def main() -> None:
    files = slices_from_env()

    with db.connect() as conn:
        db.create_staging(conn)

    total = t_copy = t_merge = 0
    t_start = time.perf_counter()
    with ProcessPoolExecutor(max_workers=WORKERS, initializer=_init_worker) as pool:
        with db.connect() as conn:
            for batch in chunks(files, CHUNK):
                t0 = time.perf_counter()
                for fut in as_completed([pool.submit(_load_one, f) for f in batch]):
                    total += fut.result()        # re-raises any worker exception here
                t_copy += time.perf_counter() - t0

                t0 = time.perf_counter()
                db.merge_staging(conn)           # combine + truncate: staging stays small
                t_merge += time.perf_counter() - t0
                print(f"  {total:,} entries loaded")

    with db.connect() as conn:
        db.finalize_staging(conn)                # cross-chunk dedup + drop staging

    print(f"  workers={WORKERS} chunk={CHUNK}  copy {t_copy:.1f}s  "
          f"merge {t_merge:.1f}s  total {time.perf_counter() - t_start:.1f}s")


if __name__ == "__main__":
    main()
