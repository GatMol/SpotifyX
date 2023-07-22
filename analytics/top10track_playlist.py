#!/usr/bin/env python
# top 10 playlist per ciascuna track

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, desc, col, row_number
from pyspark.sql.window import Window

import argparse

# create argument parser
parser = argparse.ArgumentParser()
parser.add_argument("--input", help="the input directory or file path", type=str)
parser.add_argument("--output", help="the output directory path", type=str)

# create spark session
spark = SparkSession. \
            builder. \
            config("spark.driver.host", "localhost"). \
            config("spark.executor.memory", "8g"). \
            config("spark.storage.memoryFraction", "0.2"). \
            appName("Top10Track_playlists"). \
            getOrCreate()

# parse the arguments
args = parser.parse_args()
input_file = args.input
output_dir = args.output

# read the input file
playlist_df = spark.read.json(input_file) \
                    .cache()

# for each track get all the playlists in which it appears
tracks_df = playlist_df.withColumn("track", explode("tracks"))

window = Window.partitionBy("track.track_name").orderBy(desc("num_followers"))

tracks_df = tracks_df.select("track", "name", "num_followers")

# get the top 10 playlists for each track ordered by num_followers
top10track_playlist_df = tracks_df.withColumn("rank", row_number().over(window)) \
                                    .where(col("rank") <= 10) 
                                    
top10track_playlist_df.select("track.track_name", "name", "num_followers").orderBy("track_name")


# TODO: save the output in the specified output directory
top10track_playlist_df.write.json(output_dir + "/top10track_playlist.json")