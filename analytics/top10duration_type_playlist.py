#!/usr/bin/env python
# top 10 playlist per ciascun tipo di durata (e.g. short se < 10minuti,…) con numFollower associato
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, collect_set, row_number, desc, col
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

# duration types (short, medium, long) in seconds

min_num_followers = 5
# read the input file and filter the playlists with at least 10 followers
playlist_df = spark.read.json(input_file).filter(col("num_followers") >= min_num_followers) \
                                                    .select("name", "duration_ms", "num_followers")
duration_types = {"short": {"min": 0, "max": 1000}, "medium": {"min": 1001, "max": 10000}, "long": {"min": 10001, "max": 10000000000000}}

# assign duration type to each playlist based on the duration in seconds
@udf(returnType=StringType())
def get_duration_type(duration):
    for duration_type in duration_types.keys():
        if duration_types[duration_type]["min"] <= duration <= duration_types[duration_type]["max"]:
            return duration_type

duration_df = playlist_df.withColumn("duration_type", get_duration_type(col("duration_ms")/1000))

# group playlists by duration type and collect the playlists names
playlist_window = Window.partitionBy("duration_type").orderBy(desc("num_followers"))
duration_df = duration_df.withColumn("rank", row_number().over(playlist_window)).where(col("rank") <= 10) \
                                                        .select("duration_type", "name", "num_followers", "duration_ms", "rank")

# write the result to the output directory
duration_df.write.json(output_dir + "/top10durationTypePlaylist", mode="overwrite")