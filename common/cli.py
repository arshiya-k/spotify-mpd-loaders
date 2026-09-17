"""Read the harness's env-var contract."""
import os
from pathlib import Path

from common.config import slice_files


def slices_from_env() -> list[Path]:
    n = os.environ.get("MPD_SLICES") or None
    return slice_files(int(n) if n else None)
