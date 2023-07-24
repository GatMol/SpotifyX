from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, max
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
data = lines.select(from_json(col("value").cast("string"), schema).alias("parsed_value")).select(col("parsed_value.*"))
data = data.withColumn("track", explode("tracks")).select("track.artist_name", "name", "num_followers")

out2 = data.groupBy("artist_name").max("num_followers")

dataStreamWriter = out2.writeStream.format('console').outputMode('complete')

# Query execution
query = dataStreamWriter.start()

# Query explanation
# query.explain()

# To avoid termination while streaming data continues arriving
query.awaitTermination()