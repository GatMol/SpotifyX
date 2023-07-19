from kafka import KafkaConsumer
import subprocess
import sys

# define kafka consumer
consumer = KafkaConsumer('new_playlists', bootstrap_servers='localhost:9092', value_deserializer=lambda x: x.decode('utf-8'))

# for each message in the stream appending data in hdfs
for message in consumer:
    # TODO: ci salviamo anche altro nell'hdfs? Magari info relative allo stream tipo timpestamp?
    # Get value from message and save it on hdfs
    value = message.value
    # each playlist separated by a new line to let process data using json reader of sparkSQL
    value = value + "\n" 

    # save data in local file
    # TODO: append in json? non sembra servire
    with open("/Users/davidegattini/SourceTreeProj/SpotifyX/consumer/debug/playlists_proc.json", "a") as f:
        f.write(value)

    # convert json object containing playlist in bytes (required as stdin for hdfs command)
    playlists_bytes = value.encode('utf-8')

    # save data in hdfs
    isProc = subprocess.run("$HADOOP_HOME/bin/hdfs dfs -appendToFile - /user/spotifyx/input/playlists.json", shell=True, input=playlists_bytes, check=True)
    
    if isProc: 
        print("data saved in hdfs")
    else: 
        print("error while saving data in hdfs")

    sys.stdin = sys.__stdin__