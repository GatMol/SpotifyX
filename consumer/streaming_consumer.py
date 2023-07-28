from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, TimestampType
import sys, os
# get absolute path of project root folder
projectRootPath = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, projectRootPath)
from analytics.streaming import bestPlaylist4Artist, playlist2followers, trendArtists

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

# call the streaming processing functions
playlist2followersStreamWriter = playlist2followers.playlist2followers(data)
trendArtistsStreamWriter = trendArtists.trendArtists(data)
bestPlaylist4ArtistStreamWriter = bestPlaylist4Artist.bestPlaylist4Artist(data)

# start all streaming processing
playlist2followersQuery = playlist2followersStreamWriter.start()
trendArtistQuery = trendArtistsStreamWriter.start()
bestPlaylist4ArtistQuery = bestPlaylist4ArtistStreamWriter.start()

# await for all queries started
playlist2followersQuery.awaitTermination()
trendArtistQuery.awaitTermination()
bestPlaylist4ArtistQuery.awaitTermination()