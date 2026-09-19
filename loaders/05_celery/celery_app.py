"""Celery application + the one task. Workers import this module; so does the producer.

Run a worker:   celery -A loaders.05_celery.celery_app worker --concurrency=8
"""
import os
import sys
from pathlib import Path

import billiard
from celery import Celery

# billiard (Celery's multiprocessing) defaults to spawn on macOS, but Celery's prefork
# pool initializes per-worker state in the parent and expects fork children to inherit
# it -- spawned children see an empty _localized and every task fails. Our task is
# pure Python + a Postgres socket (no Apple frameworks), so fork is safe. No-op on Linux.
if sys.platform == "darwin":
    billiard.set_start_method("fork", force=True)

from common import db
from common.copy import copy_slice_to_staging
from common.parse import parse_slice

BROKER_URL = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379/0")

app = Celery("mpd", broker=BROKER_URL, backend=BROKER_URL)
app.conf.update(
    task_serializer="json",           # arguments cross the wire as JSON, not pickle
    result_serializer="json",
    accept_content=["json"],
    task_acks_late=True,              # ack AFTER the task finishes -> a worker dying mid-slice
                                      # leaves the message in the queue for another worker
    worker_prefetch_multiplier=1,     # pull one task at a time; tasks are big and uniform,
                                      # so prefetching just makes load balancing worse
)

# One connection per worker process, opened lazily on the first task.
_conn = None


def _get_conn():
    global _conn
    if _conn is None or _conn.closed:
        _conn = db.connect()
    return _conn


@app.task(autoretry_for=(Exception,), retry_backoff=True, max_retries=3)
def load_slice(path: str) -> int:
    """Same body as loader 04's _load_one. Path is a str because it must be JSON-serializable."""
    rows = parse_slice(Path(path))
    conn = _get_conn()
    copy_slice_to_staging(conn, rows)
    conn.commit()
    return len(rows.entries)
