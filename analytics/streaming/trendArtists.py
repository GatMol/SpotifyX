from pyspark.sql.functions import explode
from mongoConfig import mongo_uri

def trendArtists(df):
    """Return artists with more tracks in all playlists"""

    # explode tracks column (each row represent a track in playlists)
    tracks_df = df.withColumn("track", explode("tracks")).select("timestamp", "track.artist_name", "name", "num_followers", "num_tracks")

    # group by artist, counting how many tracks have in all playlists
    artist2numTracks = tracks_df.groupBy(tracks_df.artist_name).count().withColumnRenamed("count", "num_artist_tracks_in_all_playlists")

    # sort (needed???)
    # orderedResult = artist2numTracks.orderBy(col("num_artist_tracks_in_all_playlists").desc())

    # return orderedResult.writeStream.format('console').outputMode('complete')
    return artist2numTracks.writeStream.format("mongodb") \
                                        .option("checkpointLocation", "/tmp/pyspark/") \
                                        .option("forceDeleteTempCheckpointLocation", "true") \
                                        .option("spark.mongodb.connection.uri", mongo_uri) \
                                        .option("spark.mongodb.database", "spotifyx") \
                                        .option("spark.mongodb.collection", "trendArtists") \
                                        .outputMode("complete")