#!/usr/bin/env python3
# top 10 playlist per gruppo di 5 tracks (ricorda di calcolare il gruppo all’interno della singola playlist)
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, collect_set, row_number, desc, col
from pyspark.sql.window import Window
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import udf
import itertools
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
            config("spark.storage.memoryFraction", "0"). \
            config("shuffle.memoryFraction", "0"). \
            appName("Top10Track_playlists"). \
            getOrCreate()

# parse the arguments
args = parser.parse_args()
input_file = args.input
output_dir = args.output

# read the input file
playlist_df = spark.read.json(input_file) 

# explode the tracks column
tracks_df = playlist_df.withColumn("track", explode("tracks"))

# calculate every group of 5 tracks for each playlist
# group by playlist and calculate all the track_names in the playlist
playlist_df = tracks_df.groupBy("name").agg(collect_set("track.track_name").alias("track_names")) \
                        .join(tracks_df, "name").select("name", "num_followers", "track_names")

# UDF to calculate the group of 5 tracks
# con 5 crasha
@udf(ArrayType(ArrayType(StringType())))
def calculate_group(tracks):
    group = []
    group.extend(itertools.combinations(tracks, 2))
    return group

# # calculate the group of 5 tracks for each playlist
playlist_df = playlist_df.repartition("name") \
                         .withColumn("group", explode(calculate_group("track_names")))

# get the top 10 playlists for each group of 5 tracks ordered by num_followers
grouped_playlist_df = playlist_df.repartition("group") \
                                    .withColumn("rank", row_number().over(Window.partitionBy("group").orderBy(desc("num_followers")))) \
                                    .where(col("rank") <= 10) \

# write the output file
grouped_playlist_df.write.json(output_dir + "/top10group_tracks_playlist.json", mode="overwrite")
