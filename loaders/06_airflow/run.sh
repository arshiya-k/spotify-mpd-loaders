#!/usr/bin/env bash
# Loader 06: Airflow. Assumes the Airflow stack is already up:
#   docker compose -f loaders/06_airflow/docker-compose.airflow.yml up -d
# Triggers the DAG through the REST API and waits for it. Workers run inside Docker
# and write to the native Postgres via host.docker.internal.
set -euo pipefail
exec .venv/bin/python loaders/06_airflow/trigger.py
