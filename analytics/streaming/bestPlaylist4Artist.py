# data = data.withColumn("track", explode("tracks")).select("track.artist_name", "name", "num_followers", "num_tracks")

# # top listened artists (with more tracks in playlists and more followers of it)
# # group by artist name and count the number of tracks in a playlist
# data = data.groupBy("artist_name", "name", "num_followers", "num_tracks").count().withColumnRenamed("count", "num_artist_tracks")

# # needed to calculate the listened artist index avoiding large numbers 
# @udf(DoubleType())
# def calculateIndex(followers, artistTracks, totTrack):
#     w1 = artistTracks / totTrack
#     return w1 * log(followers) * artistTracks

# # calculate listened artist index (number of tracks in playlists * log10(number of followers) / 250 (max number of tracks in a playlist))
# out2 = data.withColumn("index", calculateIndex(col("num_followers"), col("num_artist_tracks"), col("num_tracks")))

# # get max index per artist
# maxIdxPerArtist = out2.groupBy("artist_name").max("index").withColumnRenamed("max(index)", "maxIndex").withColumnRenamed("artist_name", "artistName")

# # get the artist with the max index
# out2 = out2.join(maxIdxPerArtist, expr("""artist_name==artistName""")).filter(col("index") == col("maxIndex")).select("artist_name", "name", "num_followers", "num_artist_tracks", "num_tracks")

# dataStreamWriter = out2.writeStream.format('console').trigger(processingTime='5 seconds')