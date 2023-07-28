from pyspark.sql.functions import explode

def trendTracks(df):
    """Return tracks more present in all playlists"""

    # explode tracks column (each row represent a track in playlists)
    tracks_df = df.withColumn("track", explode("tracks")).select("timestamp", "track.track_name", "name", "num_followers", "num_tracks")

    # group by track and count how many are present in all playlists
    track2numTracks = tracks_df.groupBy(tracks_df.track_name).count().withColumnRenamed("count", "num_tracks_in_all_playlists")

    return track2numTracks.writeStream.format("mongodb") \
                                        .option("checkpointLocation", "/tmp/pyspark/") \
                                        .option("forceDeleteTempCheckpointLocation", "true") \
                                        .option("spark.mongodb.connection.uri", "mongodb://localhost") \
                                        .option("spark.mongodb.database", "spotifyx") \
                                        .option("spark.mongodb.collection", "trendTracks") \
                                        .outputMode("complete")