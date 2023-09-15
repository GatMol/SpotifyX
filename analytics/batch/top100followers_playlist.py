#!/usr/bin/env python
# top 100 (per num_followers) spotify playlists con almeno 10 tracks
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc
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

# read the input file
args = parser.parse_args()
input_file = args.input
output_collection = args.output

# create spark session
spark = SparkSession \
            .builder \
            .config("spark.driver.host", "localhost") \
            .config("spark.executor.memory", "6g") \
            .appName("Top100Followers") \
            .config("checkpointLocation", "/tmp/pyspark/") \
            .config("forceDeleteTempCheckpointLocation", "true") \
            .config("spark.mongodb.connection.uri", mongo_uri) \
            .config("spark.mongodb.database", "spotifyx") \
            .config("spark.mongodb.collection", output_collection) \
            .getOrCreate()

min_num_followers = 10
min_num_tracks = 10
input_df = spark.read.json(input_file).filter(col("num_followers") >= min_num_followers).filter(col("num_tracks") >= min_num_tracks)

playlist_df = input_df.sort(desc("num_followers")).limit(100)

playlist_df.write.format("mongodb").mode("overwrite").save()