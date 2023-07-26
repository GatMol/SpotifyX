from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, log10
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType

# define spark session
spark = SparkSession.builder.appName('StreamingConsumer').getOrCreate()

# read stream from kafka using spark
lines = spark.readStream.format('kafka').option('kafka.bootstrap.servers', 'localhost:9092').option('subscribe', 'json-topic').load()

# define schema for the streaming data
schema = StructType([
                StructField("name", StringType()),
                StructField("collaborative", StringType()),
                StructField("pid", IntegerType()),
                StructField("modified_at", StringType()),
                StructField("num_tracks", IntegerType()),
                StructField("num_albums", IntegerType()),
                StructField("num_followers", IntegerType()),
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
data = lines.select(from_json(col("value").cast("string"), schema).alias("parsed_value")).select(col("parsed_value.*")).filter(col("num_followers") > 100)

# TODO: metti un limite minimo di followers e magari fai un'analisi su quante playlist con pochi followers sono arrivate (per capire su quante, sono state fatte le analisi/sono inutili)

data = data.withColumn("track", explode("tracks")).select("track.artist_name", "name", "num_followers")

# top listened artists (with more tracks in playlists and more followers of it)
# group by artist name and count the number of tracks in a playlist
data = data.groupBy("artist_name", "name", "num_followers").count().withColumnRenamed("count", "num_artist_tracks")

# calculate listened artist index (number of tracks in playlists * log10(number of followers) / 250 (max number of tracks in a playlist))
out2 = data.withColumn("index", col("num_artist_tracks") * log10(col("num_followers")) / 250)

# out2 = out2.sort(col("artist_name").desc(), col("index").desc())

dataStreamWriter = out2.writeStream.format('console').outputMode('complete')

# Query execution
query = dataStreamWriter.start()

# Query explanation
# query.explain()

# To avoid termination while streaming data continues arriving
query.awaitTermination()