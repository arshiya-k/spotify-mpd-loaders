"""Loader 02: naive Python. Parse each slice, INSERT row by row via executemany,
let Postgres handle cross-slice dedup with ON CONFLICT DO NOTHING.

This is the "first thing you'd write" baseline. Its cost is one round-trip
per row and a unique-index probe per dimension row.
"""
from common import db
from common.cli import slices_from_env
from common.parse import parse_slice

# ON CONFLICT needs a unique index to detect conflicts against. Our schema deliberately
# has none during load, so this loader must create them up front -- paying per-row index
# maintenance for the whole load. That's the honest cost of "let the DB dedup".
PRE_INDEXES = """
    ALTER TABLE artists ADD PRIMARY KEY (artist_uri);
    ALTER TABLE albums  ADD PRIMARY KEY (album_uri);
    ALTER TABLE tracks  ADD PRIMARY KEY (track_uri);
"""

INSERT_ARTIST   = "INSERT INTO artists VALUES (%s, %s) ON CONFLICT DO NOTHING"
INSERT_ALBUM    = "INSERT INTO albums VALUES (%s, %s, %s) ON CONFLICT DO NOTHING"
INSERT_TRACK    = "INSERT INTO tracks VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING"
INSERT_PLAYLIST = ("INSERT INTO playlists VALUES "
                   "(%s, %s, %s, %s, to_timestamp(%s), %s, %s, %s, %s, %s, %s)")
INSERT_ENTRY    = "INSERT INTO playlist_tracks VALUES (%s, %s, %s)"


def main() -> None:
    files = slices_from_env()
    with db.connect() as conn:
        conn.execute(PRE_INDEXES)
        conn.commit()

        for i, path in enumerate(files, 1):
            rows = parse_slice(path)
            with conn.cursor() as cur:
                cur.executemany(INSERT_ARTIST, rows.artists.values())
                cur.executemany(INSERT_ALBUM, rows.albums.values())
                cur.executemany(INSERT_TRACK, rows.tracks.values())
                cur.executemany(INSERT_PLAYLIST, rows.playlists)
                cur.executemany(INSERT_ENTRY, rows.entries)
            conn.commit()                      # one transaction per slice
            if i % 50 == 0:
                print(f"  {i} slices")


if __name__ == "__main__":
    main()
