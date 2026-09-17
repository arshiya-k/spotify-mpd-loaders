-- Applied after load. Each ALTER validates the whole table in one pass
-- (hash join / sort) rather than per-row during insert.
-- Order matters: a FK needs the referenced column to already be a PK/UNIQUE.

-- Some loaders (ON CONFLICT-based) must create dimension PKs up front.
-- Skip a PK that already exists so the harness's finalize step stays idempotent.
DO $$
DECLARE
    spec RECORD;
BEGIN
    FOR spec IN
        SELECT * FROM (VALUES
            ('artists',         '(artist_uri)'),
            ('albums',          '(album_uri)'),
            ('tracks',          '(track_uri)'),
            ('playlists',       '(pid)'),
            ('playlist_tracks', '(pid, pos)')
        ) AS t(tbl, cols)
    LOOP
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conrelid = spec.tbl::regclass AND contype = 'p'
        ) THEN
            EXECUTE format('ALTER TABLE %I ADD PRIMARY KEY %s', spec.tbl, spec.cols);
        END IF;
    END LOOP;
END $$;

ALTER TABLE albums          ADD FOREIGN KEY (artist_uri) REFERENCES artists (artist_uri);
ALTER TABLE tracks          ADD FOREIGN KEY (album_uri)  REFERENCES albums  (album_uri);
ALTER TABLE playlist_tracks ADD FOREIGN KEY (pid)        REFERENCES playlists (pid);
ALTER TABLE playlist_tracks ADD FOREIGN KEY (track_uri)  REFERENCES tracks (track_uri);

-- Secondary index: "which playlists contain track X?" is the obvious analytics
-- query and the PK (pid, pos) can't answer it.
CREATE INDEX playlist_tracks_track_uri_idx ON playlist_tracks (track_uri);
