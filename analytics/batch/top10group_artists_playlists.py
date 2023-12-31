#!/usr/bin/env python
# top 10 playlist per gruppo di 3 artisti 
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, collect_set, row_number, desc, col
from pyspark.sql.window import Window
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType

import itertools
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
spark = SparkSession \
            .builder \
            .config("spark.driver.host", emr_IP) \
            .appName("Top10groupArtists_playlists") \
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
grouped_playlist_df.write.format("mongodb").mode("overwrite").save()