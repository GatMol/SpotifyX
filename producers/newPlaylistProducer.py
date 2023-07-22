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
max_offset = 950 # the maximum offset allowed


# create Kafka producer to send messages to the topic 'json-topic'
producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'), bootstrap_servers=['localhost:9092'])

# for each letter in the alphabet search for new playlists 
# sleep for second_to_sleep seconds between requests and send the results to kafka
for letter in alfabeto:
    while offset < max_offset:
        searched_playlist = sp.search(letter, type="playlist", limit=limit, offset=offset)
        offset += limit

        ps = searched_playlist["playlists"]["items"]
        playlist = {}
        # for each playlist in the search result, if it is not already in the database, search it,
        # send it to kafka and add it to the database
        for aug_playlist in ps:
            if aug_playlist["id"] in playlist_ids["ids"]:
                continue
            got_playlist = sp.playlist(aug_playlist["id"])

            # fullfill the playlist dictionary with the information from the spotify api
            playlist["name"] = got_playlist["name"]
            playlist["collaborative"] = got_playlist["collaborative"]
            playlist["pid"] = got_playlist["id"]
            # add current unix timestamp as the creation time of the playlist
            playlist["modified_at"] = int(time.time())
            playlist["num_tracks"] = got_playlist["tracks"]["total"]
            playlist["num_albums"] = len(set([item["track"]["album_name"] for item in got_playlist["tracks"]["items"]]))
            playlist["num_followers"] = got_playlist["followers"]["total"]
            playlist["tracks"] = []
            i = 0
            # fullfill the tracks list of the playlist dictionary with the information from the spotify api
            for item in got_playlist["tracks"]["items"]:
                track = {}
                track["pos"] = i
                track["artist_name"] = item["track"]["artists"][0]["name"]
                track["track_uri"] = item["track"]["uri"]
                track["artist_uri"] = item["track"]["artists"][0]["uri"]
                track["track_name"] = item["track"]["name"]
                track["album_uri"] = item["track"]["album"]["uri"]
                track["duration_ms"] = item["track"]["duration_ms"]
                track["album_name"] = item["track"]["album"]["name"]

                playlist["tracks"].append(track)
                i += 1

            producer.send('json-topic', playlist)
            playlist_ids["ids"].append(playlist["pid"])
            json.dump(playlist_ids, open(playlist_path, "w"))

        time.sleep(seconds_to_sleep)