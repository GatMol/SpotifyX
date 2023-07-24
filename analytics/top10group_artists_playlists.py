#!/usr/bin/env python
# top 10 playlist per gruppo di 3 artisti 
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
            config("spark.driver.memory", "16g"). \
            config("spark.executor.memory", "16g"). \
            config("spark.sql.broadcastTimeout", "36000"). \
            appName("Top10groupArtists_playlists"). \
            getOrCreate()

# parse the arguments
args = parser.parse_args()
input_file = args.input
output_dir = args.output

# read the input file
playlist_df = spark.read.json(input_file, multiLine=True).filter("num_followers > 9")

# explode the artist_name column
artists_df = playlist_df.withColumn("artist_name", explode("tracks.artist_name")).dropDuplicates(["artist_name", "name"])

# TODO: prendi le playlist con almeno 3 artisti

# calculate every group of 3 artists for each playlist
# group by playlist and calculate all the artist_name in the playlist
playlist_df = artists_df.groupBy("name", "num_followers").agg(collect_set("artist_name").alias("artist_names"))

# UDF to calculate the group of 3 artists
@udf(ArrayType(ArrayType(StringType())))
def calculate_group(artists):
    group = []
    group.extend(itertools.combinations(artists, 3))
    return group

# calculate the group of 3 artists for each playlist
playlist_df = playlist_df.withColumn("groupArtists", explode(calculate_group("artist_names"))) \
                         .select("groupArtists", "name", "num_followers")

# get the top 10 playlists for each group of 5 tracks ordered by num_followers
window_spec = Window.partitionBy("groupArtists").orderBy(desc("num_followers"))

grouped_playlist_df = playlist_df.withColumn("rank", row_number().over(window_spec)).where(col("rank") <= 10)

# order by group
grouped_playlist_df = grouped_playlist_df.orderBy("groupArtists")
# write the output file
grouped_playlist_df.write.json(output_dir + "/top10group_artists_playlist.json", mode="overwrite")