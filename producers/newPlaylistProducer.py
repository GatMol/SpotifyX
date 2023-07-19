import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import json
import os
import string
import time
import random
from kafka import KafkaProducer

random.seed(42)

auth_manager = SpotifyClientCredentials()
sp = spotipy.Spotify(auth_manager=auth_manager)

# use this to get the playlists of a user or a category or the featured playlists
# playlist_spotify = sp.user_playlists("spotify")
# category_playlists = sp.category_playlists("rock")
# featured_playlists = sp.featured_playlists()

# create a playlist ids list including all the playlists already in the database
current_dir = os.path.dirname(os.path.realpath(__file__))
playlist_path = current_dir + "/playlist_ids.json"
if not os.path.exists(playlist_path):
    playlist_ids = {"ids": []}
else:
    playlist_ids = json.load(open(playlist_path, "r"))

# randomize the order of the alphabet
alfabeto = list(string.ascii_lowercase)
random.shuffle(alfabeto)

limit = 50  # the number of items to return
offset = 0 # the index of the first item to return
seconds_to_sleep = 30 # seconds to wait between requests


# create Kafka producer to send messages to the topic 'json-topic'
producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'), bootstrap_servers=['localhost:9092'])

# for each letter in the alphabet search for new playlists 
# sleep for second_to_sleep seconds between requests and send the results to kafka
for letter in alfabeto:
    while offset < 950:
        searched_playlist = sp.search(letter, type="playlist", limit=limit, offset=offset)
        offset += limit

        ps = searched_playlist["playlists"]["items"]
        # for each playlist in the search result, if it is not already in the database, search it,
        # send it to kafka and add it to the database
        for aug_playlist in ps:
            if aug_playlist["id"] in playlist_ids["ids"]:
                continue
            playlist = sp.playlist(aug_playlist["id"])
            producer.send('json-topic', playlist)
            playlist_ids["ids"].append(playlist["id"])
            json.dump(playlist_ids, open(playlist_path, "w"))

        time.sleep(seconds_to_sleep)