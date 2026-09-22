#!/usr/bin/env bash
# Loader 01: pure SQL. Postgres reads the JSON files off the filesystem itself with
# pg_read_file(), parses to jsonb, flattens with jsonb_array_elements, dedups with GROUP BY.
# No client-side parsing at all -- psql only ships SQL text.
#
# pg_read_file() on absolute paths needs superuser (the `mpd` role owns this cluster).
set -euo pipefail

export LC_ALL=en_US.UTF-8
PSQL="/opt/homebrew/opt/postgresql@16/bin/psql -p 5433 -U mpd -d mpd -q -v ON_ERROR_STOP=1"

# Slice files in pid order (sort numerically on the start-pid field), optionally limited.
# awk, not head: head exits early, which SIGPIPEs sort and trips `set -o pipefail`.
files=$(ls "$MPD_DATA_DIR"/mpd.slice.*.json | sort -t. -k3,3n)
if [ -n "${MPD_SLICES:-}" ]; then
    files=$(printf '%s\n' "$files" | awk -v n="$MPD_SLICES" 'NR <= n')
fi

# Phase 1: read + parse every file into raw_slices. Generate all the INSERTs as one SQL
# script and feed it to a single psql session -- one process, one connection.
{
    echo "DROP TABLE IF EXISTS raw_slices, entries_staging;"
    echo "CREATE UNLOGGED TABLE raw_slices (body jsonb);"
    for f in $files; do
        echo "INSERT INTO raw_slices SELECT pg_read_file('$f')::jsonb;"
    done
} | $PSQL

# Phase 2: explode + dedup.
$PSQL < loaders/01_sql/finalize.sql
