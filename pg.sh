#!/usr/bin/env bash
# Manage the native (Homebrew) Postgres used for benchmarking.
#   ./pg.sh start | stop | status | psql [args...]
set -euo pipefail
export LC_ALL=en_US.UTF-8
PG=/opt/homebrew/opt/postgresql@16/bin
DATA="$(cd "$(dirname "$0")/.." && pwd)/pgdata_native"

case "${1:-}" in
  start)  "$PG/pg_ctl" -D "$DATA" -l "$DATA/server.log" start ;;
  stop)   "$PG/pg_ctl" -D "$DATA" stop ;;
  status) "$PG/pg_ctl" -D "$DATA" status ;;
  psql)   shift; exec "$PG/psql" -p 5433 -U mpd -d mpd "$@" ;;
  *)      echo "usage: $0 {start|stop|status|psql}" >&2; exit 1 ;;
esac
