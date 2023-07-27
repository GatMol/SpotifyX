#!/usr/bin/env python
# miglior playlist(per numero di tracks nella playlist e num_followers) per ogni artista
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, row_number, desc, col, log, count
from pyspark.sql.window import Window
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import udf
import argparse

# create argument parser
parser = argparse.ArgumentParser()
parser.add_argument("--input", help="the input directory or file path", type=str)
parser.add_argument("--output", help="the output directory path", type=str)

# create spark session
spark = SparkSession. \
            builder. \
            config("spark.driver.host", "localhost"). \
            config("spark.driver.memory", "10g"). \
            config("spark.executor.memory", "10g"). \
            config("spark.sql.broadcastTimeout", "36000"). \
            appName("Top10durationTypePlaylist"). \
            getOrCreate()

# parse the arguments
args = parser.parse_args()
input_file = args.input
output_dir = args.output

# read the input file and filter the playlists with at least 10 followers
min_num_followers = 10
playlist_df = spark.read.json(input_file).filter(col("num_followers") >= min_num_followers)

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

artist_playlist_df.write.json(output_dir + "/bestArtistPlaylist", mode="overwrite")