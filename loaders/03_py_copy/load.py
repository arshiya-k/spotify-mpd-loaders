"""Loader 03: Python + COPY. Same parsing as 02, but rows stream to Postgres via
the COPY protocol (one command, raw tuples) and cross-slice dedup is an in-memory
set of URIs already sent -- no unique index needed during load.

Single process, so the "seen" sets are authoritative. This breaks the moment
there are two processes; that's loader 04's problem.
"""
from collections.abc import Iterable

import psycopg
from psycopg import sql

from common import db
from common.cli import slices_from_env
from common.parse import (ALBUM_COLS, ARTIST_COLS, ENTRY_COLS, PLAYLIST_COLS, TRACK_COLS,
                          parse_slice)


def copy_rows(conn: psycopg.Connection, table: str, cols: tuple[str, ...], rows: Iterable[tuple]) -> None:
    """Stream rows into `table` with COPY. psycopg handles type adaptation per row."""
    stmt = sql.SQL("COPY {} ({}) FROM STDIN").format(
        sql.Identifier(table),
        sql.SQL(", ").join(map(sql.Identifier, cols)),
    )
    with conn.cursor() as cur, cur.copy(stmt) as copy:
        for row in rows:
            copy.write_row(row)


def new_only(rows: dict[str, tuple], seen: set[str]) -> list[tuple]:
    """Rows whose key hasn't been sent yet; marks them seen."""
    fresh = [r for k, r in rows.items() if k not in seen]
    seen.update(rows.keys())
    return fresh


def main() -> None:
    files = slices_from_env()
    seen_tracks: set[str] = set()
    seen_albums: set[str] = set()
    seen_artists: set[str] = set()

    with db.connect() as conn:
        for i, path in enumerate(files, 1):
            rows = parse_slice(path)
            copy_rows(conn, "artists", ARTIST_COLS, new_only(rows.artists, seen_artists))
            copy_rows(conn, "albums", ALBUM_COLS, new_only(rows.albums, seen_albums))
            copy_rows(conn, "tracks", TRACK_COLS, new_only(rows.tracks, seen_tracks))
            copy_rows(conn, "playlists", PLAYLIST_COLS, to_playlist_rows(rows.playlists))
            copy_rows(conn, "playlist_tracks", ENTRY_COLS, rows.entries)
            conn.commit()
            if i % 50 == 0:
                print(f"  {i} slices")


def to_playlist_rows(playlists: list[tuple]) -> Iterable[tuple]:
    """COPY can't call to_timestamp(); convert modified_at to a datetime client-side."""
    from datetime import datetime, timezone
    for p in playlists:
        yield (*p[:4], datetime.fromtimestamp(p[4], tz=timezone.utc), *p[5:])


if __name__ == "__main__":
    main()
