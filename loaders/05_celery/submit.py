"""Producer: enqueue one task per slice, wait for all to finish, then finalize.

This process does no loading. It could exit right after apply_async() and the
workers would still drain the queue -- we only block so the harness can time it.
"""
import time

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


def main() -> None:
    files = slices_from_env()
    with db.connect() as conn:
        db.create_staging(conn)

    t0 = time.perf_counter()
    job = group(load_slice.s(str(f)) for f in files)
    result = job.apply_async()
    print(f"  enqueued {len(files)} tasks in {time.perf_counter() - t0:.2f}s")

    counts = result.get(disable_sync_subtasks=False)     # blocks until every task has a result
    t_parallel = time.perf_counter() - t0
    print(f"  {len(counts)} tasks done, {sum(counts):,} entries, {t_parallel:.1f}s")

    t0 = time.perf_counter()
    with db.connect() as conn:
        db.finalize_staging(conn)
    print(f"  finalize {time.perf_counter() - t0:.1f}s")


if __name__ == "__main__":
    main()
