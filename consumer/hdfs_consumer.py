from kafka import KafkaConsumer
import subprocess
import sys
from consumer.awsConfig import *

# define kafka consumer
# consumer = KafkaConsumer('playlist-topic', bootstrap_servers='localhost:9092', value_deserializer=lambda x: x.decode('utf-8'))
# kafka consumer deployed on aws emr
consumer = KafkaConsumer('playlist-topic', bootstrap_servers=kafka_broker_IP+":"+kafka_port, value_deserializer=lambda x: x.decode('utf-8'))

# TODO: divide json files so that contains only 1000 playlists avoiding to have a single file with all the playlists???

# for each message in the stream append data in a single json file saved on hdfs
for message in consumer:
    # Get value from message and save it on hdfs
    value = message.value
    # each playlist separated by a new line to let process data using json reader of sparkSQL
    value = value + "\n" 

    # convert json object containing playlist in bytes (required as stdin for hdfs command)
    playlists_bytes = value.encode('utf-8')

    # save data in hdfs
    isProc = subprocess.run("$HADOOP_HOME/bin/hdfs dfs -appendToFile - /user/spotifyx/input/playlists.json", shell=True, input=playlists_bytes, check=True)

    if isProc: 
        print("data saved in hdfs")
    else: 
        print("error while saving data in hdfs")

    sys.stdin = sys.__stdin__