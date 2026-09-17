-- Staging for multi-writer loaders. Workers append here without coordination;
-- finalize_staging.sql collapses duplicates into the real dimension tables.
-- UNLOGGED: no WAL. If Postgres crashes mid-load we re-run from the JSON anyway.

DROP TABLE IF EXISTS artists_staging, albums_staging, tracks_staging;

CREATE UNLOGGED TABLE artists_staging (LIKE artists);
CREATE UNLOGGED TABLE albums_staging  (LIKE albums);
CREATE UNLOGGED TABLE tracks_staging  (LIKE tracks);
