#!/usr/bin/env python
# top 10 playlist per ciascun tipo di durata (e.g. short se < 10minuti,…) con numFollower associato
from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number, desc, col
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType

import argparse

from mongoConfig import mongo_uri
from awsConfig import emr_IP

# create argument parser
parser = argparse.ArgumentParser()
parser.add_argument("--input", help="the input directory or file path", type=str)
parser.add_argument("--output", help="the output collection name", type=str)

# parse the arguments
args = parser.parse_args()
input_file = args.input
output_collection = args.output

# create spark session
spark = SparkSession \
            .builder \
            .config("spark.driver.host", emr_IP) \
            .config("spark.driver.memory", "10g") \
            .config("spark.executor.memory", "10g") \
            .config("spark.sql.broadcastTimeout", "36000") \
            .appName("Top10durationTypePlaylist") \
            .config("checkpointLocation", "/tmp/pyspark/") \
            .config("forceDeleteTempCheckpointLocation", "true") \
            .config("spark.mongodb.connection.uri", mongo_uri) \
            .config("spark.mongodb.database", "spotifyx") \
            .config("spark.mongodb.collection", output_collection) \
            .getOrCreate()

# Define json schema (to make it work in AWS)
schema = StructType([
                StructField("name", StringType()),
                StructField("collaborative", StringType()),
                StructField("pid", IntegerType()),
                StructField("modified_at", StringType()),
                StructField("num_tracks", IntegerType()),
                StructField("num_albums", IntegerType()),
                StructField("num_followers", IntegerType()),
                StructField("duration_ms", IntegerType()),
                StructField("num_artists", IntegerType()),
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

# duration types (short, medium, long) in seconds
duration_types = {"short": {"min": 0, "max": 1000}, "medium": {"min": 1000, "max": 10000}, "long": {"min": 10000, "max": 10000000000000}}

min_num_followers = 5
# read the input file and filter the playlists with at least 10 followers
playlist_df = spark.read.json(input_file).filter(col("num_followers") >= min_num_followers) \
                                                    .select("name", "duration_ms", "num_followers")

# assign duration type to each playlist based on the duration in seconds
@udf(returnType=StringType())
def get_duration_type(duration):
    for duration_type in duration_types.keys():
        if duration_types[duration_type]["min"] <= duration < duration_types[duration_type]["max"]:
            return duration_type

duration_df = playlist_df.withColumn("duration_type", get_duration_type(col("duration_ms")/1000))

# group playlists by duration type and collect the playlists names
playlist_window = Window.partitionBy("duration_type").orderBy(desc("num_followers"))
duration_df = duration_df.withColumn("rank", row_number().over(playlist_window)).where(col("rank") <= 10) \
                                                        .select("duration_type", "name", "num_followers", "duration_ms", "rank")

# write the result to the output directory
duration_df.write.format("mongodb").mode("overwrite").save()