#!/usr/bin/env python
# top 100 (per num_followers) spotify playlists con almeno 10 tracks
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc
import argparse

# create argument parser
parser = argparse.ArgumentParser()
parser.add_argument("--input", help="the input directory or file path", type=str)
parser.add_argument("--output", help="the output directory path", type=str)

# create spark session
spark = SparkSession. \
            builder. \
            config("spark.driver.host", "localhost"). \
            appName("Top100Followers"). \
            getOrCreate()

# read the input file
args = parser.parse_args()
input_file = args.input
output_dir = args.output

input_df = spark.read.json(input_file).cache()

playlist_df = input_df.select("*").where(col("num_tracks") > 10)

playlist_df = playlist_df.sort(desc("num_followers")).limit(100)

playlist_df.show()

playlist_df.write.json(output_dir + "/top100followers_playlist.json", mode="overwrite")