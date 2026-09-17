"""Ground-truth counts for validation.

For a full run these come from the dataset's own stats.txt. For a partial run
(--slices N) we compute them by scanning the JSON -- there's no shortcut, since
"unique tracks in the first N slices" depends on which tracks those are.
"""
import json
from pathlib import Path

from common.config import DATA_DIR, slice_files

FULL_DATASET = {
    "playlists": 1_000_000,
    "playlist_tracks": 66_346_428,
    "tracks": 2_262_292,
    "albums": 734_684,
    "artists": 295_860,
}


def expected_counts(n_slices: int | None) -> dict[str, int]:
    if n_slices is None or n_slices >= 1000:
        return FULL_DATASET

    tracks, albums, artists = set(), set(), set()
    n_playlists = n_entries = 0
    for path in slice_files(n_slices):
        for pl in json.loads(path.read_text())["playlists"]:
            n_playlists += 1
            for t in pl["tracks"]:
                n_entries += 1
                tracks.add(t["track_uri"])
                albums.add(t["album_uri"])
                artists.add(t["artist_uri"])
    return {
        "playlists": n_playlists,
        "playlist_tracks": n_entries,
        "tracks": len(tracks),
        "albums": len(albums),
        "artists": len(artists),
    }
