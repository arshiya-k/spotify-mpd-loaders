-- Dedup staging -> real tables. GROUP BY (not DISTINCT ON) so the planner can use
-- a parallel hash aggregate. min() is safe: every URI has exactly one name.

INSERT INTO artists (artist_uri, name)
SELECT artist_uri, min(name) FROM artists_staging GROUP BY artist_uri;

INSERT INTO albums (album_uri, name, artist_uri)
SELECT album_uri, min(name), min(artist_uri) FROM albums_staging GROUP BY album_uri;

INSERT INTO tracks (track_uri, name, album_uri, duration_ms)
SELECT track_uri, min(name), min(album_uri), min(duration_ms) FROM tracks_staging GROUP BY track_uri;

DROP TABLE artists_staging, albums_staging, tracks_staging;
