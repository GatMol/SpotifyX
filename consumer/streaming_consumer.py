from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, udf, expr
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, DoubleType, TimestampType
from math import log
import sys, os
# get absolute path of project root folder
projectRootPath = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, projectRootPath)
from analytics.streaming import orderPlaylistByFollowers, bestPlaylist4Artist, trendArtists

# define spark session
spark = SparkSession.builder.appName('StreamingConsumer').getOrCreate()

# read stream from kafka using spark
lines = spark.readStream.format('kafka').option('kafka.bootstrap.servers', 'localhost:9092').option('subscribe', 'json-topic').load()

# define schema for the streaming data
schema = StructType([
                StructField("timestamp", TimestampType()),
                StructField("name", StringType()),
                StructField("collaborative", StringType()),
                StructField("pid", IntegerType()),
                StructField("modified_at", StringType()),
                StructField("num_tracks", IntegerType()),
                StructField("num_albums", IntegerType()),
                StructField("num_followers", IntegerType()),
                StructField("duration_ms", IntegerType()),
                StructField("tracks", ArrayType(StructType([
                    StructField("pos", IntegerType()),
                    StructField("artist_name", StringType()),
                    StructField("track_uri", StringType()),
                    StructField("artist_uri", StringType()),
                    StructField("track_name", StringType()),
                    StructField("album_uri", StringType()),
                    StructField("duration_ms", IntegerType()),
                    StructField("album_name", StringType())
                ])))
            ])

# convert the data from binary to string and then to json
# kafka stream data are composed of key, value, topic, partition, offset, timestamp, timestampType
# then select the parsed value and expand it to columns
data = lines.select(from_json(col("value").cast("string"), schema).alias("parsed_value")).select(col("parsed_value.*"))

# order playlists by number of followers
# orderedPlaylistsStreamWriter = orderPlaylistByFollowers.orderPlaylistByFollowers(data)
trendArtistsStreamWriter = trendArtists.trendArtists(data)

# start all streaming processing
# orderedPlaylistsStreamWriter.start()
trendArtistQuery = trendArtistsStreamWriter.start()

# await for all queries started
# orderedPlaylistsStreamWriter.awaitTermination()
trendArtistQuery.awaitTermination()