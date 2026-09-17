#!/usr/bin/env bash
# Smoke test: proves the harness contract works. Not a real benchmark entry.
set -euo pipefail
exec .venv/bin/python loaders/00_smoke/load.py
