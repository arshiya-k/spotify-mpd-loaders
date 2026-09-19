#!/usr/bin/env bash
# Loader 05: Celery. Starts N worker processes (in the background), enqueues one task
# per slice via Redis, waits, finalizes, then stops the workers.
#
# In a real deployment the workers would already be running -- possibly on other
# machines -- and this script would only be the "submit" half.
set -euo pipefail

WORKERS="${MPD_WORKERS:-8}"
CELERY=.venv/bin/celery

# purge leftovers from a previous failed run so we don't load stale tasks
(cd loaders/05_celery && ../../$CELERY -A celery_app purge -f >/dev/null 2>&1) || true

# Celery's main process shuts its forked children down on TERM. pkill -P catches
# any stragglers. `|| true` so cleanup never turns a successful run into a failure.
(cd loaders/05_celery && exec ../../$CELERY -A celery_app worker \
    --concurrency="$WORKERS" --loglevel=warning --without-gossip --without-mingle) &
WORKER_PID=$!
cleanup() {
    kill -TERM "$WORKER_PID" 2>/dev/null || true
    sleep 1
    pkill -P "$WORKER_PID" 2>/dev/null || true
    wait "$WORKER_PID" 2>/dev/null || true
}
trap cleanup EXIT

# give the worker a moment to connect to Redis before we flood the queue
sleep 2

PYTHONPATH=. .venv/bin/python loaders/05_celery/submit.py
