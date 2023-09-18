#!/usr/bin/env python
# top 10 playlist per ciascun artist

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, desc, col, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType

from mongoConfig import mongo_uri
from awsConfig import emr_IP

import argparse

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
            .config("spark.executor.memory", "8g") \
            .config("spark.storage.memoryFraction", "0.2") \
            .appName("Top10Artist_playlists") \
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

# read the input file
playlist_df = spark.read.json(input_file) \
                    .cache()

# for each artist get all the playlists in which it appears, and remove duplicates (same playlist can have multiple songs of the same artist)
artist_df = playlist_df.withColumn("artist_name", explode("tracks.artist_name")).dropDuplicates(["name", "artist_name"])

window = Window.partitionBy("artist_name").orderBy(desc("num_followers"))

artist_df = artist_df.select("artist_name", "name", "num_followers")

# get the top 10 playlists for each artist ordered by num_followers
top10artist_playlist_df = artist_df.withColumn("rank", row_number().over(window)) \
                                    .where(col("rank") <= 10) 

# order by artist_name
top10artist_playlist_df.orderBy("artist_name")

top10artist_playlist_df.write.format("mongodb").mode("overwrite").save()