#!/usr/bin/env python
# top 10 playlist per ciascuna track

from pyspark.sql import SparkSession
import argparse

# create argument parser
parser = argparse.ArgumentParser()
parser.add_argument("--input", help="the input directory or file path", type=str)
parser.add_argument("--output", help="the output directory path", type=str)

# create spark session
spark = SparkSession. \
            builder. \
            config("spark.driver.host", "localhost"). \
            appName("Top10Track_playlists"). \
            getOrCreate()

# parse the arguments
args = parser.parse_args()
input_file = args.input
output_dir = args.output

# read the input file
input_df = spark.read.json(input_file).cache()

# get the playlists
playlist_df = input_df.select("playlists")

# get for each track all the playlists in which it appears
# TODO: termina lo script 
