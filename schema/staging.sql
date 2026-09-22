-- Staging for multi-writer loaders. Two levels, both UNLOGGED (no WAL; we can
-- re-run from the JSON if Postgres crashes) and both WITHOUT indexes.
--
--   *_staging   raw dimension rows as workers write them -- every duplicate.
--               Truncated after each chunk, so it never exceeds one chunk's worth.
--   *_merged    per-chunk GROUP BY results accumulated across chunks. Still holds
--               duplicates ACROSS chunks, but far fewer rows than raw staging.
--
-- This is the combiner pattern: aggregate locally to shrink what the final
-- aggregation has to read. The alternative -- deduping straight into the real
-- tables per chunk -- needs primary keys during load, and per-row index
-- maintenance measured 5.6x slower (9.0s vs 1.6s) than one bulk build at the end.

DROP TABLE IF EXISTS artists_staging, albums_staging, tracks_staging,
                     artists_merged, albums_merged, tracks_merged;

CREATE UNLOGGED TABLE artists_staging (LIKE artists);
CREATE UNLOGGED TABLE albums_staging  (LIKE albums);
CREATE UNLOGGED TABLE tracks_staging  (LIKE tracks);

CREATE UNLOGGED TABLE artists_merged (LIKE artists);
CREATE UNLOGGED TABLE albums_merged  (LIKE albums);
CREATE UNLOGGED TABLE tracks_merged  (LIKE tracks);
