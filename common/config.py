"""Environment-driven settings. Everything that differs per machine lives here."""
import os
from pathlib import Path

from dotenv import load_dotenv

# Walk up from this file to find .env at the repo root, regardless of cwd.
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

DATA_DIR = Path(os.environ["MPD_DATA_DIR"])
DATABASE_URL = os.environ["DATABASE_URL"]

SCHEMA_DIR = Path(__file__).resolve().parents[1] / "schema"


def slice_files(limit: int | None = None) -> list[Path]:
    """All slice files in playlist-id order. `limit` takes the first N for dev runs.

    Sorted numerically by the start pid, not lexically -- "mpd.slice.10000-10999"
    would otherwise sort before "mpd.slice.2000-2999".
    """
    files = sorted(
        DATA_DIR.glob("mpd.slice.*.json"),
        key=lambda p: int(p.name.split(".")[2].split("-")[0]),
    )
    return files[:limit] if limit else files
