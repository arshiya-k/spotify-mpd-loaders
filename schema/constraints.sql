-- Applied after load. Each ALTER validates the whole table in one pass
-- (hash join / sort) rather than per-row during insert.
-- Order matters: a FK needs the referenced column to already be a PK/UNIQUE.

ALTER TABLE artists         ADD PRIMARY KEY (artist_uri);
ALTER TABLE albums          ADD PRIMARY KEY (album_uri);
ALTER TABLE tracks          ADD PRIMARY KEY (track_uri);
ALTER TABLE playlists       ADD PRIMARY KEY (pid);
ALTER TABLE playlist_tracks ADD PRIMARY KEY (pid, pos);

ALTER TABLE albums          ADD FOREIGN KEY (artist_uri) REFERENCES artists (artist_uri);
ALTER TABLE tracks          ADD FOREIGN KEY (album_uri)  REFERENCES albums  (album_uri);
ALTER TABLE playlist_tracks ADD FOREIGN KEY (pid)        REFERENCES playlists (pid);
ALTER TABLE playlist_tracks ADD FOREIGN KEY (track_uri)  REFERENCES tracks (track_uri);

-- Secondary index: "which playlists contain track X?" is the obvious analytics
-- query and the PK (pid, pos) can't answer it.
CREATE INDEX playlist_tracks_track_uri_idx ON playlist_tracks (track_uri);
