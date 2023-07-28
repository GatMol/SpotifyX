#!/usr/bin/env python
# top 10 playlist per ciascuna track

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, desc, col, row_number
from pyspark.sql.window import Window

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
# configuration derived from https://stackoverflow.com/questions/21138751/spark-java-lang-outofmemoryerror-java-heap-space
spark = SparkSession \
            .builder \
            .config("spark.driver.host", "localhost") \
            .config("spark.executor.memory", "8g") \
            .config("spark.storage.memoryFraction", "0") \
            .config("shuffle.memoryFraction", "0") \
            .appName("Top10Track_playlists") \
            .option("checkpointLocation", "/tmp/pyspark/") \
            .option("forceDeleteTempCheckpointLocation", "true") \
            .option("spark.mongodb.connection.uri", "mongodb://localhost") \
            .option("spark.mongodb.database", "spotifyx") \
            .option("spark.mongodb.collection", output_collection) \
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
top10track_playlist_df.write.format("mongo").mode("overwrite").save()