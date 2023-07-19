import os
import json
from multiprocessing import Pool
import asyncio
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import threading
import os

scope = "user-library-read"

spotify = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials())
def get_track_features(track):
    
    track_uri = track["track_uri"]
    artist_uri = track["artist_uri"]
    artist_dict = {}
    try:
        artist_features = spotify.artist(artist_uri)
        print("Getting artist features for: " + artist_uri)
        artist_dict["deleted"] = False
        artist_dict["genres"] = artist_features["genres"]
        artist_dict["popularity"] = artist_features["popularity"]
        artist_dict["followers"] = artist_features["followers"]["total"]
    except:
        artist_dict["deleted"] = True
        print("Artist not found: " + artist_uri)
    try:
        track_features = spotify.track(track_uri)
        print("Getting track features for: " + track_uri)
        track["artist"] = artist_dict
        track["deleted"] = False
        track["popularity"] = track_features["popularity"]
        track["explicit"] = track_features["explicit"]
        track["release_date"] = track_features["album"]["release_date"]
    except:
        track["deleted"] = True
        print("Track not found: " + track_uri)
    return track

def get_playlist_tracks(playlist):
    tracks = []
    
    for track in playlist["tracks"]:

        tracks.append(get_track_features(track))
    return tracks

    

def get_track_features_multiprocessing(file_path, file):
    df = json.load(open(file_path))
    playlists = []
    for playlist in df["playlists"]:
        
        playlist["tracks"] = get_playlist_tracks(playlist)
        playlists.append(playlist)
    df["playlists"] = playlists
    json.dump(df, open(augmented_dataset_dir + file, "w"))

def get_playlist_for_files(start, end, dataset_dir):
    for file in os.listdir(dataset_dir)[start:end]:
        print("Processing file: " + file)
        get_track_features_multiprocessing(dataset_dir + file, file)
     
if __name__ == "__main__":

    dataset_dir = "dataset/data/"
    augmented_dataset_dir = "dataset/augmented_data/"
    
    num_cores = os.cpu_count() - 1
    threads = []

    print("Starting data augmentation with " + str(num_cores) + " cores...")

    file_per_core = len(os.listdir(dataset_dir)) // num_cores
    for i in range(num_cores - 1):
        t = threading.Thread(target=get_playlist_for_files, args=(i * file_per_core, (i + 1) * file_per_core, dataset_dir))
        threads.append(t)
        t.start()

    t = threading.Thread(target=get_playlist_for_files, args=((num_cores - 1) * file_per_core, len(os.listdir(dataset_dir)), dataset_dir))
    threads.append(t)
    t.start()

    for thread in threads:
        thread.join()

    print("Done!")