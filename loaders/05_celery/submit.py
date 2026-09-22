"""Producer: enqueue one task per slice, wait for all to finish, then finalize.

This process does no loading. It could exit right after apply_async() and the
workers would still drain the queue -- we only block so the harness can time it.

Work is enqueued in chunks, with a staging merge between chunks, for the same
reason loader 04 does it: staging absorbs every duplicate dimension row, so
loading all 1000 slices before collapsing would put ~8 GB of soon-to-be-discarded
rows on disk. Chunking bounds that to one chunk's worth regardless of input size.
"""
import os
import time
from itertools import islice

from celery import group

from common import db
from common.cli import slices_from_env

# "05_celery" isn't a valid package name, so load the task module by path.
import importlib.util, sys
from pathlib import Path
_spec = importlib.util.spec_from_file_location("celery_app", Path(__file__).with_name("celery_app.py"))
celery_app = importlib.util.module_from_spec(_spec)
sys.modules["celery_app"] = celery_app
_spec.loader.exec_module(celery_app)
load_slice = celery_app.load_slice


CHUNK = int(os.environ.get("MPD_CHUNK") or 100)      # slices per staging merge


def chunks(seq, size):
    it = iter(seq)
    while batch := list(islice(it, size)):
        yield batch


def main() -> None:
    files = slices_from_env()
    with db.connect() as conn:
        db.create_staging(conn)

    total = 0
    t_start = time.perf_counter()
    with db.connect() as conn:
        for batch in chunks(files, CHUNK):
            job = group(load_slice.s(str(f)) for f in batch)
            counts = job.apply_async().get(disable_sync_subtasks=False)
            total += sum(counts)
            db.merge_staging(conn)               # combine + truncate: staging stays small
            print(f"  {total:,} entries loaded")
    print(f"  {len(files)} tasks done, {total:,} entries, {time.perf_counter() - t_start:.1f}s")

    t0 = time.perf_counter()
    with db.connect() as conn:
        db.finalize_staging(conn)
    print(f"  finalize {time.perf_counter() - t0:.1f}s")


if __name__ == "__main__":
    main()
