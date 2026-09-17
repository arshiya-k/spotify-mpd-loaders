-- Explode raw_slices -> normalized tables. One pass over the parsed JSON.

-- Playlists: one row per playlist object.
INSERT INTO playlists
SELECT
    (p->>'pid')::int,
    p->>'name',
    p->>'description',
    (p->>'collaborative')::boolean,
    to_timestamp((p->>'modified_at')::bigint),
    (p->>'num_tracks')::int,
    (p->>'num_albums')::int,
    (p->>'num_artists')::int,
    (p->>'num_followers')::int,
    (p->>'num_edits')::int,
    (p->>'duration_ms')::bigint
FROM raw_slices, jsonb_array_elements(body->'playlists') AS p;

-- Staging: one row per (playlist, track), fully denormalized. 66M rows at full scale.
CREATE UNLOGGED TABLE entries_staging AS
SELECT
    (p->>'pid')::int  AS pid,
    (t->>'pos')::int  AS pos,
    t->>'track_uri'   AS track_uri,  t->>'track_name'  AS track_name, (t->>'duration_ms')::int AS duration_ms,
    t->>'album_uri'   AS album_uri,  t->>'album_name'  AS album_name,
    t->>'artist_uri'  AS artist_uri, t->>'artist_name' AS artist_name
FROM raw_slices,
     jsonb_array_elements(body->'playlists') AS p,
     jsonb_array_elements(p->'tracks') AS t;

DROP TABLE raw_slices;

-- Dedup via GROUP BY rather than DISTINCT ON: lets the planner use a parallel hash
-- aggregate instead of a full sort. min() is safe -- each URI has exactly one name.
INSERT INTO artists (artist_uri, name)
SELECT artist_uri, min(artist_name) FROM entries_staging GROUP BY artist_uri;

INSERT INTO albums (album_uri, name, artist_uri)
SELECT album_uri, min(album_name), min(artist_uri) FROM entries_staging GROUP BY album_uri;

INSERT INTO tracks (track_uri, name, album_uri, duration_ms)
SELECT track_uri, min(track_name), min(album_uri), min(duration_ms) FROM entries_staging GROUP BY track_uri;

INSERT INTO playlist_tracks (pid, pos, track_uri)
SELECT pid, pos, track_uri FROM entries_staging;

DROP TABLE entries_staging;
