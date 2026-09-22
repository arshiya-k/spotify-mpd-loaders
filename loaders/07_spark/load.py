"""Loader 07: Spark. The only loader where the flatten and the dedup happen OUTSIDE
Postgres -- Spark parses the JSON, explodes the nesting, and dedups across its own
workers, then writes five finished tables over JDBC.

The dedup is a shuffle: Spark hashes track_uri and routes every row with the same
hash to the same partition, so all copies of a track meet on one worker and can be
collapsed locally. Same idea as Postgres's hash aggregate, but across the cluster.
"""
import os
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (ArrayType, BooleanType, IntegerType, LongType, StringType,
                               StructField, StructType)

DATA_DIR = Path(os.environ["MPD_DATA_DIR"])
N_SLICES = int(os.environ.get("MPD_SLICES") or 0) or None
JDBC_URL = "jdbc:postgresql://localhost:5433/mpd"
JDBC_PROPS = {"user": "mpd", "driver": "org.postgresql.Driver",
              "batchsize": "10000", "reWriteBatchedInserts": "true"}

# An explicit schema skips Spark's schema-inference pass, which would otherwise read
# every file once just to work out the field types. On 31GB that pass is not cheap.
TRACK = StructType([
    StructField("pos", IntegerType()),
    StructField("track_uri", StringType()),
    StructField("track_name", StringType()),
    StructField("duration_ms", IntegerType()),
    StructField("album_uri", StringType()),
    StructField("album_name", StringType()),
    StructField("artist_uri", StringType()),
    StructField("artist_name", StringType()),
])
PLAYLIST = StructType([
    StructField("pid", IntegerType()),
    StructField("name", StringType()),
    StructField("description", StringType()),
    StructField("collaborative", StringType()),     # "true"/"false" strings in the source
    StructField("modified_at", LongType()),
    StructField("num_tracks", IntegerType()),
    StructField("num_albums", IntegerType()),
    StructField("num_artists", IntegerType()),
    StructField("num_followers", IntegerType()),
    StructField("num_edits", IntegerType()),
    StructField("duration_ms", LongType()),
    StructField("tracks", ArrayType(TRACK)),
])
SCHEMA = StructType([StructField("playlists", ArrayType(PLAYLIST))])


def slice_paths() -> list[str]:
    files = sorted(DATA_DIR.glob("mpd.slice.*.json"),
                   key=lambda p: int(p.name.split(".")[2].split("-")[0]))
    return [str(p) for p in (files[:N_SLICES] if N_SLICES else files)]


def write(df, table: str) -> None:
    df.write.mode("append").jdbc(JDBC_URL, table, properties=JDBC_PROPS)


def main() -> None:
    spark = (SparkSession.builder
             .appName("mpd-load")
             .master(f"local[{os.environ.get('MPD_WORKERS', '8')}]")
             .config("spark.jars", "jars/postgresql.jar")
             .config("spark.driver.memory", "4g")
             .config("spark.sql.shuffle.partitions", "16")   # default 200 is far too many at this size
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # multiLine: each file is ONE json object spanning many lines, not one object per line.
    raw = spark.read.schema(SCHEMA).option("multiLine", "true").json(slice_paths())

    # explode turns one row with an array of N into N rows -- the flattening step every
    # loader does, here as a dataframe operation instead of a Python loop or a SQL function.
    pl = raw.select(F.explode("playlists").alias("p")).select("p.*")
    entries = pl.select("pid", F.explode("tracks").alias("t"))

    playlists = pl.select(
        "pid", "name", "description",
        (F.col("collaborative") == "true").alias("collaborative"),
        F.to_timestamp(F.col("modified_at")).alias("modified_at"),
        "num_tracks", "num_albums", "num_artists", "num_followers", "num_edits", "duration_ms",
    )

    # cache: `entries` feeds four separate writes. Without this Spark would re-read and
    # re-explode the JSON for each one, since dataframes are lazy and recomputed on demand.
    entries = entries.select(
        "pid", F.col("t.pos").alias("pos"), F.col("t.track_uri").alias("track_uri"),
        F.col("t.track_name").alias("track_name"), F.col("t.duration_ms").alias("duration_ms"),
        F.col("t.album_uri").alias("album_uri"), F.col("t.album_name").alias("album_name"),
        F.col("t.artist_uri").alias("artist_uri"), F.col("t.artist_name").alias("artist_name"),
    ).cache()

    artists = entries.select("artist_uri", F.col("artist_name").alias("name")).dropDuplicates(["artist_uri"])
    albums = entries.select("album_uri", F.col("album_name").alias("name"), "artist_uri").dropDuplicates(["album_uri"])
    tracks = entries.select("track_uri", F.col("track_name").alias("name"), "album_uri", "duration_ms").dropDuplicates(["track_uri"])

    write(artists, "artists")
    write(albums, "albums")
    write(tracks, "tracks")
    write(playlists, "playlists")
    write(entries.select("pid", "pos", "track_uri"), "playlist_tracks")

    spark.stop()


if __name__ == "__main__":
    main()
