import json
import os
import time
import random
from kafka import KafkaProducer

random.seed(42)

# create a playlist ids list including all the playlists already in the database
current_dir = os.path.dirname(os.path.realpath(__file__))
playlist_path = current_dir + "/from_data_playlist_ids.json"
if not os.path.exists(playlist_path):
    playlist_ids = {"ids": []}
else:
    playlist_ids = json.load(open(playlist_path, "r"))

streaming_data_path = current_dir + "/../dataset/streaming_data"

batch_size = 50
time_to_sleep = 30 # seconds to wait between requests

# create Kafka producer to send messages to the topic 'json-topic'
producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'), bootstrap_servers=['localhost:9092'])

# get the list of files in the streaming_data folder and shuffle it
files = os.listdir(streaming_data_path)
random.shuffle(files)

# for each file in the streaming_data folder, navigate through the playlists and send them to kafka
for file in files:
    current_batch_size = 0
    
    playlist_file = json.load(open(streaming_data_path + "/" + file, "r"))
    playlists = playlist_file["playlists"]
    for playlist in playlists:
        
        # if the playlist is already in the database, skip it
        if playlist["pid"] in playlist_ids["ids"]:
            continue
        producer.send('json-topic', playlist)
        playlist_ids["ids"].append(playlist["pid"])
        current_batch_size += 1

        # every batch_size playlists, sleep for time_to_sleep seconds
        if current_batch_size == batch_size:
            time.sleep(time_to_sleep)
            current_batch_size = 0