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
            config("spark.executor.memory", "16g"). \
            appName("Top100Followers"). \
            getOrCreate()

# read the input file
args = parser.parse_args()
input_file = args.input
output_dir = args.output

min_num_followers = 10
min_num_tracks = 10
input_df = spark.read.json(input_file).filter(col("num_followers") >= min_num_followers).filter(col("num_tracks") >= min_num_tracks)

playlist_df = input_df.sort(desc("num_followers")).limit(100)

playlist_df.write.json(output_dir + "/top100followers_playlist.json", mode="overwrite")