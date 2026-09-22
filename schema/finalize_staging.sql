-- Final dedup pass: merged tables -> real dimension tables.
-- GROUP BY (not DISTINCT ON) so the planner can use a parallel hash aggregate.
-- min() is safe: every URI has exactly one name in this dataset.
--
-- Reads *_merged when the loader used chunked merges, *_staging when it wrote
-- everything in one pass. Either way this is the only insert into the real tables,
-- so their primary keys are still built in bulk afterwards by constraints.sql.

INSERT INTO artists (artist_uri, name)
SELECT artist_uri, min(name) FROM artists_merged GROUP BY artist_uri;

INSERT INTO albums (album_uri, name, artist_uri)
SELECT album_uri, min(name), min(artist_uri) FROM albums_merged GROUP BY album_uri;

INSERT INTO tracks (track_uri, name, album_uri, duration_ms)
SELECT track_uri, min(name), min(album_uri), min(duration_ms) FROM tracks_merged GROUP BY track_uri;

DROP TABLE artists_staging, albums_staging, tracks_staging,
           artists_merged, albums_merged, tracks_merged;
