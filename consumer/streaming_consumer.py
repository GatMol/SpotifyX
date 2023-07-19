from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# define spark session
spark = SparkSession.builder.appName('StreamingConsumer').getOrCreate()

# read stream from kafka using spark
lines = spark.readStream.format('kafka').option('kafka.bootstrap.servers', 'localhost:9092').option('subscribe', 'new_playlists').load()

# define schema for the streaming data
schema = StructType([
            StructField("playlists", ArrayType(StructType([
                StructField("name", StringType()),
                StructField("collaborative", StringType()),
                StructField("pid", StringType()),
                StructField("modified_at", StringType())
            ]))),
            StructField("tracks", ArrayType(StructType([
                StructField("pos", StringType())
            ])))]
        )

# QUERY DEFINITION
# convert the data from binary to string and then to json
# kafka stream data are composed of key, value, topic, partition, offset, timestamp, timestampType
# TODO: capisci cosa è la chiave
out = lines.withColumn("value", from_json(col("value").cast("string"), schema=schema)).select("value")

# data preparation
out2 = out.select("value.playlists.name", "value.playlists.collaborative")

# data processing
# TODO: processamento e analisi dei dati

# QUERY EXECUTION
qr1 = out2.writeStream.format('console').start()

# To avoid termination while streaming data continues arriving
qr1.awaitTermination()