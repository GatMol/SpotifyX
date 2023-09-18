#!/usr/bin/env python
# miglior playlist(per numero di tracks nella playlist e num_followers) per ogni artista
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, row_number, desc, col, log, count
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType

import argparse

from mongoConfig import mongo_uri
from awsConfig import emr_IP

# create argument parser
parser = argparse.ArgumentParser()
parser.add_argument("--input", help="the input directory or file path", type=str)
parser.add_argument("--output", help="the output collection name", type=str)

# parse the arguments
args = parser.parse_args()
input_file = args.input
output_collection = args.output

# create spark session
# REMEMBER TO CHANGE LOCALHOST TO PUBLIC IP OF EMR INSTANCE WHEN USING AWS (leave localhost if running locally)
spark = SparkSession \
            .builder \
            .config("spark.driver.host", emr_IP) \
            .config("spark.driver.memory", "10g") \
            .config("spark.executor.memory", "10g") \
            .config("spark.sql.broadcastTimeout", "36000") \
            .appName("best_artist_playlist") \
            .config("checkpointLocation", "/tmp/pyspark/") \
            .config("forceDeleteTempCheckpointLocation", "true") \
            .config("spark.mongodb.connection.uri", mongo_uri) \
            .config("spark.mongodb.database", "spotifyx") \
            .config("spark.mongodb.collection", output_collection) \
            .getOrCreate()

# Define json schema (to make it work in AWS)
schema = StructType([
                StructField("name", StringType()),
                StructField("collaborative", StringType()),
                StructField("pid", IntegerType()),
                StructField("modified_at", StringType()),
                StructField("num_tracks", IntegerType()),
                StructField("num_albums", IntegerType()),
                StructField("num_followers", IntegerType()),
                StructField("duration_ms", IntegerType()),
                StructField("num_artists", IntegerType()),
                StructField("tracks", ArrayType(StructType([
                    StructField("pos", IntegerType()),
                    StructField("artist_name", StringType()),
                    StructField("track_uri", StringType()),
                    StructField("artist_uri", StringType()),
                    StructField("track_name", StringType()),
                    StructField("album_uri", StringType()),
                    StructField("duration_ms", IntegerType()),
                    StructField("album_name", StringType())
                ])))
            ])

# read the input file and filter the playlists with at least 10 followers
min_num_followers = 10
playlist_df = spark.read.json(input_file, schema=schema).filter(col("num_followers") >= min_num_followers)

# group playlists by artist and collect the playlists names
artist_playlist_df = playlist_df.withColumn("artist", explode("tracks.artist_name")).select("artist", "name", "num_followers")

# define function to calculate the playlist rating
def calculate_rating(num_followers, num_tracks):
    return log(num_followers) * num_tracks

# calculate num of tracks of the artist in the playlist
artist_playlist_df = artist_playlist_df.withColumn("artist_num_tracks", count("name").over(Window.partitionBy("artist", "name")))

# calculate the rating of the playlist
artist_playlist_df = artist_playlist_df.withColumn("rating", calculate_rating(col("num_followers"), col("artist_num_tracks")))

# define artisti window
artist_window = Window.partitionBy("artist").orderBy(desc("rating"))

# calculate the best playlist for each artist
artist_playlist_df = artist_playlist_df.withColumn("row_number", row_number().over(artist_window)).filter(col("row_number") == 1).drop("row_number")

artist_playlist_df.show()

artist_playlist_df.write.format("mongodb").mode("overwrite").save()