from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, window, bround


def calculate_avg_stats(data):

    # calculate average of:
    #  - number of tracks in a playlist
    #  - number of albums in a playlist
    #  - number of followers of a playlist
    #  - duration of a playlist
    #  - number of artists in a playlist
    windowed_data = data.groupBy(
        window(data.timestamp, "10 seconds", "5 seconds")
    ).agg(bround(avg("num_tracks"), 2).alias("avg_num_tracks"),
          bround(avg("num_albums"), 2).alias("avg_num_albums"), 
          bround(avg("num_followers"), 2).alias("avg_num_followers"), 
          bround(avg("duration_ms"), 2).alias("avg_duration_ms"), 
          bround(avg("num_artists"), 2).alias("avg_num_artists"))

    # TODO: write the result to mongodb
    dataStreamWriter = windowed_data.writeStream.format('console').outputMode('complete')
    return dataStreamWriter