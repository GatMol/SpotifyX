from pyspark.sql.functions import max

def playlist2followers(df):
    playlist2followers = df.select("name", "num_followers")
    return playlist2followers.writeStream.format("mongodb") \
                                        .option("checkpointLocation", "/tmp/pyspark/") \
                                        .option("forceDeleteTempCheckpointLocation", "true") \
                                        .option("spark.mongodb.connection.uri", "mongodb://localhost") \
                                        .option("spark.mongodb.database", "spotifyx") \
                                        .option("spark.mongodb.collection", "playlist2followers")