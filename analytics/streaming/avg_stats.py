from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, avg


def calculate_avg_stats(data):
        
    # calculate average of:
    #  - number of tracks in a playlist
    #  - number of albums in a playlist
    #  - number of followers of a playlist
    #  - duration of a playlist
    #  - number of artists in a playlist
    avg_data = data.select(avg("num_tracks"), avg("num_albums"), avg("num_followers"), avg("duration_ms"), avg("num_artists"))

    # TODO: write the result to mongodb
    dataStreamWriter = avg_data.writeStream.format('console').outputMode('complete').trigger(processingTime='5 seconds')
    return dataStreamWriter