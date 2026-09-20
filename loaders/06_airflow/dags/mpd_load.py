"""Loader 06: the same load as 04/05, expressed as an Airflow DAG.

    create_staging  >>  load_slice[0..N]  >>  finalize  >>  validate
                        (dynamic task mapping: one task instance per slice file)

Every task is a function from common/. Airflow contributes dependency enforcement,
per-task retries, run history, and selective re-runs -- not speed.
"""
import os
from pathlib import Path

from airflow.sdk import dag, task

from common import db
from common.config import slice_files
from common.copy import copy_slice_to_staging
from common.parse import parse_slice


@dag(
    dag_id="mpd_load",
    schedule=None,                 # triggered manually / via API, not on a timer
    catchup=False,
    is_paused_upon_creation=False,   # Airflow pauses new DAGs by default; runs of a paused DAG sit queued forever
    params={"slices": 10},         # override at trigger time: {"slices": 1000}
    tags=["spotify-mpd"],
)
def mpd_load():

    @task
    def create_staging() -> None:
        with db.connect() as conn:
            db.create_staging(conn)

    @task
    def list_slices(**context) -> list[str]:
        """Return the slice paths as strings -- task outputs must be JSON-serializable."""
        n = int(context["params"]["slices"]) or None
        return [str(p) for p in slice_files(n)]

    @task(retries=2, retry_delay=30)
    def load_slice(path: str) -> int:
        """Same body as loaders 04/05, including the idempotency guard."""
        rows = parse_slice(Path(path))
        with db.connect() as conn:
            first_pid = rows.playlists[0][0]
            if conn.execute("SELECT 1 FROM playlists WHERE pid = %s", (first_pid,)).fetchone():
                return 0
            copy_slice_to_staging(conn, rows)
        return len(rows.entries)

    @task
    def finalize(counts: list[int]) -> int:
        with db.connect() as conn:
            db.finalize_staging(conn)
        return sum(counts)

    @task
    def validate(total_entries: int) -> None:
        """Fail the run loudly if the fact table doesn't match what the workers reported."""
        with db.connect() as conn:
            actual = conn.execute("SELECT count(*) FROM playlist_tracks").fetchone()[0]
        if actual != total_entries:
            raise ValueError(f"playlist_tracks has {actual:,} rows, workers loaded {total_entries:,}")

    staging = create_staging()
    paths = list_slices()
    counts = load_slice.expand(path=paths)      # <-- one task instance per path, run in parallel
    staging >> paths                            # don't list/load until staging tables exist
    validate(finalize(counts))


mpd_load()
