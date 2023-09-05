from pyspark.sql.functions import explode, udf, log, col
from pyspark.sql.types import DoubleType
from mongoConfig import mongo_uri

def bestPlaylist4Artist(df):
    tracks_df = df.withColumn("track", explode("tracks")).select("track.artist_name", "name", "num_followers", "num_tracks")

    # top listened artists (with more tracks in playlists and more followers of it)
    # group by artist name and count the number of tracks in a playlist
    artistPlaylist2numTracks = tracks_df.groupBy("artist_name", "name", "num_followers", "num_tracks").count().withColumnRenamed("count", "num_artist_tracks")

    # needed to calculate the listened artist index avoiding large numbers 
    @udf(DoubleType())
    def calculateIndex(followers, artistTracks, totTrack):
        w1 = artistTracks / totTrack
        return w1 * log(followers) * artistTracks

    # calculate listened artist index (number of tracks in playlists * log10(number of followers) / 250 (max number of tracks in a playlist))
    artistPlaylist2index = artistPlaylist2numTracks.withColumn("index", calculateIndex(col("num_followers"), col("num_artist_tracks"), col("num_tracks")))

    return artistPlaylist2index.writeStream.format("mongodb") \
                                        .option("checkpointLocation", "/tmp/pyspark/") \
                                        .option("forceDeleteTempCheckpointLocation", "true") \
                                        .option("spark.mongodb.connection.uri", mongo_uri) \
                                        .option("spark.mongodb.database", "spotifyx") \
                                        .option("spark.mongodb.collection", "bestPlaylist4Artist") \
                                        .outputMode("complete")