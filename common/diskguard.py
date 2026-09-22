"""Pre-flight disk check. A load that fills the disk corrupts the run and can
destabilise the OS -- much better to refuse to start.

Calibration, measured not guessed:
  * A completed 100-slice load (tables + PKs + FKs + the track_uri index) occupies
    920 MB  ->  ~9.2 MB per slice.
  * On top of that, two transient costs that do NOT scale with slice count:
    one chunk of staging (~0.8 GB at the default chunk size) and the temp files
    Postgres spills while sorting the big indexes (~2 GB at full scale).
"""
import shutil
from pathlib import Path

BYTES_PER_SLICE = 9_500_000           # steady-state: tables + indexes
TRANSIENT_BYTES = 3 * 1024**3         # staging chunk + index-build temp files
SAFETY_MARGIN = 3 * 1024**3           # leave room for the OS


def required_bytes(n_slices: int) -> int:
    return n_slices * BYTES_PER_SLICE + TRANSIENT_BYTES + SAFETY_MARGIN


def check(n_slices: int, path: Path) -> None:
    need = required_bytes(n_slices)
    free = shutil.disk_usage(path).free
    if free >= need:
        return
    fits = int((free - TRANSIENT_BYTES - SAFETY_MARGIN) / BYTES_PER_SLICE)
    raise SystemExit(
        f"refusing to start: {n_slices} slices needs ~{need / 1024**3:.1f} GB "
        f"(including {(TRANSIENT_BYTES + SAFETY_MARGIN) / 1024**3:.0f} GB of transient "
        f"and safety headroom) but only {free / 1024**3:.1f} GB is free.\n"
        f"Free up space, or run --slices {max(fits, 0)} or fewer."
    )
