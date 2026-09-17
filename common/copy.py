"""COPY helpers: stream row tuples into a table over psycopg's COPY protocol."""
from collections.abc import Iterable
from datetime import datetime, timezone

import psycopg
from psycopg import sql

from common.parse import (ALBUM_COLS, ARTIST_COLS, ENTRY_COLS, PLAYLIST_COLS, TRACK_COLS,
                          SliceRows)


def copy_rows(conn: psycopg.Connection, table: str, cols: tuple[str, ...], rows: Iterable[tuple]) -> None:
    stmt = sql.SQL("COPY {} ({}) FROM STDIN").format(
        sql.Identifier(table),
        sql.SQL(", ").join(map(sql.Identifier, cols)),
    )
    with conn.cursor() as cur, cur.copy(stmt) as copy:
        for row in rows:
            copy.write_row(row)


def playlist_rows(playlists: list[tuple]) -> Iterable[tuple]:
    """COPY can't call to_timestamp(); convert modified_at client-side."""
    for p in playlists:
        yield (*p[:4], datetime.fromtimestamp(p[4], tz=timezone.utc), *p[5:])


def copy_slice_to_staging(conn: psycopg.Connection, rows: SliceRows) -> None:
    """Write one parsed slice: dims to *_staging (dedup later), facts straight to real tables.

    Used by every multi-writer loader (04, 05, 06). No cross-slice dedup here by design --
    workers can't share state, so staging absorbs duplicates and finalize collapses them.
    """
    copy_rows(conn, "artists_staging", ARTIST_COLS, rows.artists.values())
    copy_rows(conn, "albums_staging", ALBUM_COLS, rows.albums.values())
    copy_rows(conn, "tracks_staging", TRACK_COLS, rows.tracks.values())
    copy_rows(conn, "playlists", PLAYLIST_COLS, playlist_rows(rows.playlists))
    copy_rows(conn, "playlist_tracks", ENTRY_COLS, rows.entries)
