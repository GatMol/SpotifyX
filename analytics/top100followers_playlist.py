#!/usr/bin/env python

from pyspark.sql import SparkSession
import argparse

# create argument parser
parser = argparse.ArgumentParser()
parser.add_argument("--input", help="the input directory or file path", type=str)

# create spark session
spark = SparkSession. \
            builder. \
            config("spark.driver.host", "localhost"). \
            appName("Top100Followers"). \
            getOrCreate()

# read the input file
args = parser.parse_args()
input_file = args.input

playlist_df = spark.read.json(input_file).cache()

playlist_df = playlist_df.filter(playlist_df["num_tracks"] > 10)

playlist_df = playlist_df.sort(playlist_df["num_followers"].desc())

playlist_df = playlist_df.limit(100)

playlist_df.show()