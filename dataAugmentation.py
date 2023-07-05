import spotipy as sp
from spotipy.oauth2 import SpotifyOAuth
import os
import json
import pandas as pd

dataset_dir = "dataset/data/"

scope = "user-library-read"

sp = sp.Spotify(auth_manager=SpotifyOAuth(scope=scope))

final_df = pd.DataFrame()
for file in os.listdir(dataset_dir)[:1]:
    df = json.load(open(dataset_dir + file))
    for playlist in df["playlists"][:1]:
        for track in playlist["tracks"][:1]:
            track_uri = track["track_uri"]
            artist_uri = track["artist_uri"]

            track_features = sp.track(track_uri)
            artist_features = sp.artist(artist_uri)
            print("Track :")
            print(track_features)
            print("\n\n")
            print("Artist :")
            print(artist_features)