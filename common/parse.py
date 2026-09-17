"""Turn one slice file into flat row tuples. Shared by every Python loader.

Returns rows in the column order of the schema tables, so loaders can pass
them straight to INSERT / COPY without renaming.
"""
import json
from pathlib import Path
from typing import NamedTuple


class SliceRows(NamedTuple):
    playlists: list[tuple]
    entries: list[tuple]          # playlist_tracks: (pid, pos, track_uri)
    tracks: dict[str, tuple]      # keyed by uri -> deduped within the slice
    albums: dict[str, tuple]
    artists: dict[str, tuple]


def parse_slice(path: Path) -> SliceRows:
    doc = json.loads(path.read_bytes())
    playlists, entries = [], []
    tracks, albums, artists = {}, {}, {}

    for pl in doc["playlists"]:
        pid = pl["pid"]
        playlists.append((
            pid, pl["name"], pl.get("description"),
            pl["collaborative"] == "true", pl["modified_at"],
            pl["num_tracks"], pl["num_albums"], pl["num_artists"],
            pl["num_followers"], pl["num_edits"], pl["duration_ms"],
        ))
        for t in pl["tracks"]:
            entries.append((pid, t["pos"], t["track_uri"]))
            # dict assignment = "last write wins"; all writes are identical so it's a set with payload
            tracks[t["track_uri"]] = (t["track_uri"], t["track_name"], t["album_uri"], t["duration_ms"])
            albums[t["album_uri"]] = (t["album_uri"], t["album_name"], t["artist_uri"])
            artists[t["artist_uri"]] = (t["artist_uri"], t["artist_name"])

    return SliceRows(playlists, entries, tracks, albums, artists)


PLAYLIST_COLS = ("pid", "name", "description", "collaborative", "modified_at",
                 "num_tracks", "num_albums", "num_artists", "num_followers", "num_edits", "duration_ms")
ENTRY_COLS = ("pid", "pos", "track_uri")
TRACK_COLS = ("track_uri", "name", "album_uri", "duration_ms")
ALBUM_COLS = ("album_uri", "name", "artist_uri")
ARTIST_COLS = ("artist_uri", "name")
