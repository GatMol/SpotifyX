#!/usr/bin/env python
# top 10 playlist per ciascuna track

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, desc, col, row_number
from pyspark.sql.window import Window

import argparse

from mongoConfig import mongo_uri
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# create argument parser
parser = argparse.ArgumentParser()
parser.add_argument("--input", help="the input directory or file path", type=str)
parser.add_argument("--output", help="the output collection name", type=str)

# parse the arguments
args = parser.parse_args()
input_file = args.input
output_collection = args.output

# create spark session
# configuration derived from https://stackoverflow.com/questions/21138751/spark-java-lang-outofmemoryerror-java-heap-space
spark = SparkSession \
            .builder \
            .config("spark.driver.host", "localhost") \
            .config("spark.executor.memory", "6g") \
            .config("spark.storage.memoryFraction", "0") \
            .config("shuffle.memoryFraction", "0") \
            .appName("Top10Track_playlists") \
            .config("checkpointLocation", "/tmp/pyspark/") \
            .config("forceDeleteTempCheckpointLocation", "true") \
            .config("spark.mongodb.connection.uri", mongo_uri) \
            .config("spark.mongodb.database", "spotifyx") \
            .config("spark.mongodb.collection", output_collection) \
            .getOrCreate()

# read the input file
playlist_df = spark.read.json(input_file) 

# for each track get all the playlists in which it appears
tracks_df = playlist_df.withColumn("track", explode("tracks"))

tracks_df = tracks_df.select("track", "name", "num_followers")

# create a window partitioned by track_name and ordered by num_followers
window = Window.partitionBy("track.track_name").orderBy(desc("num_followers"))
# get the top 10 playlists for each track ordered by num_followers
top10track_playlist_df = tracks_df.withColumn("rank", row_number().over(window)) \
                                    .where(col("rank") <= 10) 

# select only the columns we need and order by track_name
top10track_playlist_df = top10track_playlist_df.select("track.track_name", "name", "num_followers").orderBy("track_name")

# write the output file
top10track_playlist_df.write.format("mongodb").mode("overwrite").save()