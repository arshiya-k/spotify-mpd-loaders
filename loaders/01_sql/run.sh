#!/usr/bin/env bash
# Loader 01: pure SQL. Postgres reads the JSON files itself (mounted at /data in the
# container), parses to jsonb once, flattens with jsonb_array_elements, dedups with DISTINCT ON.
set -euo pipefail

PSQL="docker exec -i mpd-postgres psql -U mpd -d mpd -q -v ON_ERROR_STOP=1"

# Slice files in pid order, optionally limited. Paths are as seen INSIDE the container.
files=$(ls "$MPD_DATA_DIR"/mpd.slice.*.json | xargs -n1 basename \
        | sort -t. -k3,3n | { if [ -n "${MPD_SLICES:-}" ]; then head -n "$MPD_SLICES"; else cat; fi; })

# Phase 1: read + parse every file into raw_slices. Generate all the INSERTs as one SQL
# script and feed it to a single psql session -- one process, one connection, one transaction.
{
    echo "DROP TABLE IF EXISTS raw_slices, entries_staging;"
    echo "CREATE UNLOGGED TABLE raw_slices (body jsonb);"
    for f in $files; do
        echo "INSERT INTO raw_slices SELECT pg_read_file('/data/$f')::jsonb;"
    done
} | $PSQL

# Phase 2: explode + dedup.
$PSQL < loaders/01_sql/finalize.sql
