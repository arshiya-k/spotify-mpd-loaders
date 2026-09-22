-- Final dedup pass: staging -> dimension tables. Self-contained, so it works for
-- loaders that merge per chunk AND for loaders that write everything in one pass.
--
-- Step 1 flushes whatever is still in raw staging into *_merged. For a chunked
-- loader staging is already empty and this is a no-op; for an unchunked one it is
-- the only combine step.
-- Step 2 collapses *_merged (which still holds cross-chunk duplicates) into the
-- real tables. GROUP BY rather than DISTINCT ON so the planner can use a parallel
-- hash aggregate; min() is safe because every URI has exactly one name.
--
-- This is the only insert into the real dimension tables, so their primary keys
-- are still built in bulk afterwards by constraints.sql.

INSERT INTO artists_merged (artist_uri, name)
SELECT artist_uri, min(name) FROM artists_staging GROUP BY artist_uri;

INSERT INTO albums_merged (album_uri, name, artist_uri)
SELECT album_uri, min(name), min(artist_uri) FROM albums_staging GROUP BY album_uri;

INSERT INTO tracks_merged (track_uri, name, album_uri, duration_ms)
SELECT track_uri, min(name), min(album_uri), min(duration_ms) FROM tracks_staging GROUP BY track_uri;

INSERT INTO artists (artist_uri, name)
SELECT artist_uri, min(name) FROM artists_merged GROUP BY artist_uri;

INSERT INTO albums (album_uri, name, artist_uri)
SELECT album_uri, min(name), min(artist_uri) FROM albums_merged GROUP BY album_uri;

INSERT INTO tracks (track_uri, name, album_uri, duration_ms)
SELECT track_uri, min(name), min(album_uri), min(duration_ms) FROM tracks_merged GROUP BY track_uri;

DROP TABLE artists_staging, albums_staging, tracks_staging,
           artists_merged, albums_merged, tracks_merged;
