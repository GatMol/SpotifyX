#!/usr/bin/env python
# top 100 (per num_followers) spotify playlists con almeno 10 tracks
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType

import argparse

from mongoConfig import mongo_uri
from awsConfig import emr_IP

# create argument parser
parser = argparse.ArgumentParser()
parser.add_argument("--input", help="the input directory or file path", type=str)
parser.add_argument("--output", help="the output collection name", type=str)

# read the input file
args = parser.parse_args()
input_file = args.input
output_collection = args.output

# create spark session
spark = SparkSession \
            .builder \
            .config("spark.driver.host", emr_IP) \
            .config("spark.executor.memory", "6g") \
            .appName("Top100Followers") \
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

min_num_followers = 10
min_num_tracks = 10
input_df = spark.read.json(input_file).filter(col("num_followers") >= min_num_followers).filter(col("num_tracks") >= min_num_tracks)

playlist_df = input_df.sort(desc("num_followers")).limit(100)

playlist_df.write.format("mongodb").mode("overwrite").save()