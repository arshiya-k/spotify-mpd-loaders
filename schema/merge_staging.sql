-- Collapse one chunk of raw staging into the merged tables, then empty staging.
-- TRUNCATE (not DELETE) returns the pages to the filesystem immediately -- that is
-- what keeps peak disk independent of how many slices there are in total.
--
-- No ON CONFLICT and no indexes here: duplicates across chunks are expected and
-- get collapsed by the final pass in finalize_staging.sql.

INSERT INTO artists_merged (artist_uri, name)
SELECT artist_uri, min(name) FROM artists_staging GROUP BY artist_uri;

INSERT INTO albums_merged (album_uri, name, artist_uri)
SELECT album_uri, min(name), min(artist_uri) FROM albums_staging GROUP BY album_uri;

INSERT INTO tracks_merged (track_uri, name, album_uri, duration_ms)
SELECT track_uri, min(name), min(album_uri), min(duration_ms) FROM tracks_staging GROUP BY track_uri;

TRUNCATE artists_staging, albums_staging, tracks_staging;
