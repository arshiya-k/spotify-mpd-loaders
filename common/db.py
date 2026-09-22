"""Thin helpers around psycopg. Every loader uses these for the shared phases."""
import psycopg

from common.config import DATABASE_URL, SCHEMA_DIR


def connect(**kwargs) -> psycopg.Connection:
    return psycopg.connect(DATABASE_URL, **kwargs)


def run_sql_file(conn: psycopg.Connection, name: str) -> None:
    conn.execute((SCHEMA_DIR / name).read_text())
    conn.commit()


def reset_schema(conn: psycopg.Connection) -> None:
    """Drop and recreate bare tables. Every benchmark run starts here."""
    run_sql_file(conn, "schema.sql")


def add_constraints(conn: psycopg.Connection) -> None:
    """PKs, FKs, indexes -- after load."""
    run_sql_file(conn, "constraints.sql")


def row_counts(conn: psycopg.Connection) -> dict[str, int]:
    tables = ["artists", "albums", "tracks", "playlists", "playlist_tracks"]
    counts = {}
    for t in tables:
        counts[t] = conn.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
    return counts


def create_staging(conn: psycopg.Connection) -> None:
    run_sql_file(conn, "staging.sql")


def finalize_staging(conn: psycopg.Connection) -> None:
    """Collapse *_staging into dimension tables."""
    run_sql_file(conn, "finalize_staging.sql")


def merge_staging(conn: psycopg.Connection) -> None:
    """Collapse one chunk of raw staging into the merged tables, then truncate staging.

    Keeps the working set bounded: raw staging never exceeds one chunk, so peak disk
    is independent of how many slices there are in total.
    """
    run_sql_file(conn, "merge_staging.sql")
