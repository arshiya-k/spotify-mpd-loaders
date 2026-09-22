#!/usr/bin/env bash
# Full benchmark sweep. Loaders are grouped by the services they need, so each group
# runs with only its own dependencies up -- background containers skew timings.
#
#   ./run_benchmarks.sh 100 3      # 100 slices, 3 repetitions
set -euo pipefail

# The Airflow compose file interpolates ${MPD_DATA_DIR}; without it even `stop` fails
# with "invalid spec: :/data:ro", leaving containers running and skewing later groups.
set -a; . ./.env; set +a

SLICES="${1:-100}"
REPEAT="${2:-3}"
PY=.venv/bin/python

echo "=== group 1: no external services (01, 02, 03, 04, 07) ==="
$PY benchmark.py --slices "$SLICES" --repeat "$REPEAT" \
    01_sql 02_py_sequential 03_py_copy 04_multiproc 07_spark

echo "=== group 2: Redis only (05) ==="
docker compose up -d redis
$PY benchmark.py --slices "$SLICES" --repeat "$REPEAT" 05_celery
docker compose stop redis

echo "=== group 3: Airflow stack (06) ==="
docker compose up -d redis
docker compose -f loaders/06_airflow/docker-compose.airflow.yml up -d
sleep 30
$PY benchmark.py --slices "$SLICES" --repeat "$REPEAT" 06_airflow
docker compose -f loaders/06_airflow/docker-compose.airflow.yml stop
docker compose stop redis

echo "=== done: results/results.csv ==="
