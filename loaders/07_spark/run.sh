#!/usr/bin/env bash
# Loader 07: Spark in local[N] mode -- N threads in one JVM, standing in for a cluster.
# Same code runs on a real cluster by changing .master(); everything else is identical.
set -euo pipefail
exec .venv/bin/python loaders/07_spark/load.py
