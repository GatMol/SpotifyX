from pyspark.sql.functions import avg, window, bround
from mongoConfig import mongo_uri

def calculate_avg_stats(data):

    # calculate average of:
    #  - number of tracks in a playlist
    #  - number of albums in a playlist
    #  - number of followers of a playlist
    #  - duration of a playlist
    #  - number of artists in a playlist
    windowed_data = data.groupBy(
        window(data.timestamp, "10 minutes", "5 minutes")
    ).agg(bround(avg("num_tracks"), 2).alias("avg_num_tracks"),
          bround(avg("num_albums"), 2).alias("avg_num_albums"), 
          bround(avg("num_followers"), 2).alias("avg_num_followers"), 
          bround(avg("duration_ms"), 2).alias("avg_duration_ms"), 
          bround(avg("num_artists"), 2).alias("avg_num_artists"))

    # TODO: write the result to mongodb
    dataStreamWriter = windowed_data.writeStream.format("mongodb") \
                                        .option("checkpointLocation", "/tmp/pyspark/") \
                                        .option("forceDeleteTempCheckpointLocation", "true") \
                                        .option("spark.mongodb.connection.uri", mongo_uri) \
                                        .option("spark.mongodb.database", "spotifyx") \
                                        .option("spark.mongodb.collection", "avg_stats") \
                                        .outputMode("complete")
    return dataStreamWriter