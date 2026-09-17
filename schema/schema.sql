-- Bare tables: no primary keys, no foreign keys, no indexes.
-- Constraints are added AFTER load (see constraints.sql) so bulk inserts
-- don't pay per-row index maintenance and FK lookups.

DROP TABLE IF EXISTS playlist_tracks, playlists, tracks, albums, artists CASCADE;

CREATE TABLE artists (
    artist_uri  TEXT NOT NULL,
    name        TEXT NOT NULL
);

CREATE TABLE albums (
    album_uri   TEXT NOT NULL,
    name        TEXT NOT NULL,
    artist_uri  TEXT NOT NULL
);

CREATE TABLE tracks (
    track_uri   TEXT NOT NULL,
    name        TEXT NOT NULL,
    album_uri   TEXT NOT NULL,
    duration_ms INTEGER NOT NULL
);

CREATE TABLE playlists (
    pid           INTEGER NOT NULL,
    name          TEXT NOT NULL,
    description   TEXT,
    collaborative BOOLEAN NOT NULL,
    modified_at   TIMESTAMPTZ NOT NULL,
    num_tracks    INTEGER NOT NULL,
    num_albums    INTEGER NOT NULL,
    num_artists   INTEGER NOT NULL,
    num_followers INTEGER NOT NULL,
    num_edits     INTEGER NOT NULL,
    duration_ms   BIGINT NOT NULL
);

CREATE TABLE playlist_tracks (
    pid        INTEGER NOT NULL,
    pos        INTEGER NOT NULL,
    track_uri  TEXT NOT NULL
);
