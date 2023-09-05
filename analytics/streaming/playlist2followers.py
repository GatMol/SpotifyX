from mongoConfig import mongo_uri

def playlist2followers(df):
    playlist2followers = df.select("name", "num_followers")
    return playlist2followers.writeStream.format("mongodb") \
                                        .option("checkpointLocation", "/tmp/pyspark/") \
                                        .option("forceDeleteTempCheckpointLocation", "true") \
                                        .option("spark.mongodb.connection.uri", mongo_uri) \
                                        .option("spark.mongodb.database", "spotifyx") \
                                        .option("spark.mongodb.collection", "playlist2followers")