"""Minimal loader: dedup in Python sets, insert with executemany. One slice, no tricks."""
import json
import os

from common import db
from common.config import slice_files

n = int(os.environ.get("MPD_SLICES") or 0) or None
artists, albums, tracks, playlists, entries = {}, {}, {}, [], []

for path in slice_files(n):
    for pl in json.loads(path.read_text())["playlists"]:
        playlists.append((
            pl["pid"], pl["name"], pl.get("description"),
            pl["collaborative"] == "true", pl["modified_at"],
            pl["num_tracks"], pl["num_albums"], pl["num_artists"],
            pl["num_followers"], pl["num_edits"], pl["duration_ms"],
        ))
        for t in pl["tracks"]:
            artists[t["artist_uri"]] = (t["artist_uri"], t["artist_name"])
            albums[t["album_uri"]] = (t["album_uri"], t["album_name"], t["artist_uri"])
            tracks[t["track_uri"]] = (t["track_uri"], t["track_name"], t["album_uri"], t["duration_ms"])
            entries.append((pl["pid"], t["pos"], t["track_uri"]))

with db.connect() as conn:
    cur = conn.cursor()
    cur.executemany("INSERT INTO artists VALUES (%s, %s)", artists.values())
    cur.executemany("INSERT INTO albums VALUES (%s, %s, %s)", albums.values())
    cur.executemany("INSERT INTO tracks VALUES (%s, %s, %s, %s)", tracks.values())
    cur.executemany(
        "INSERT INTO playlists VALUES (%s, %s, %s, %s, to_timestamp(%s), %s, %s, %s, %s, %s, %s)",
        playlists,
    )
    cur.executemany("INSERT INTO playlist_tracks VALUES (%s, %s, %s)", entries)
