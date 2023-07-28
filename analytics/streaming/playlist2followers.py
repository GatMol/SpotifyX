from pyspark.sql.functions import max

def playlist2followers(df):
    playlist2followers = df.select("name", "num_followers")
    return playlist2followers.writeStream.format('console').outputMode('complete')