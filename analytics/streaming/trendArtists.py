from pyspark.sql.functions import explode, col, window

def trendArtists(df):
    """Return artists with more tracks in all playlists"""

    # explode tracks column (each row represent a track in playlists)
    tracks_df = df.withColumn("track", explode("tracks")).select("timestamp", "track.artist_name", "name", "num_followers", "num_tracks")

    # group by window and artist, counting how many tracks have in all playlists
    artist2numTracks = tracks_df.groupBy(tracks_df.artist_name).count().withColumnRenamed("count", "num_artist_tracks_in_all_playlists")

    # sort (needed???)
    # orderedResult = artist2numTracks.orderBy(col("num_artist_tracks_in_all_playlists").desc())

    # return orderedResult.writeStream.format('console').outputMode('complete')
    return artist2numTracks.writeStream.format('console').outputMode('complete')