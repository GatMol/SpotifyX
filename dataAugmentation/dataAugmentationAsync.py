import os
import asyncio
import json
from spotipy2 import Spotify
from spotipy2.auth import ClientCredentialsFlow
import time

# Artista
#   Genere
#   Popolarità
#   Followers
# Traccia
#   Artista
#   Popolarità
#   Esplicito
#   Rilascio album

# Per ogni playlist prendi le info delle singole tracce
async def augment(dataset_path, filename):
    
    client = Spotify(
        ClientCredentialsFlow(
            client_id="",
            client_secret=""
        )
    )

    # apro il dataset su cui fare augmentation
    spotify_db = json.load(open(dataset_path + filename))

    playlists_augmented = []

    async with client as s:
        # per ogni playlist nel file e per ogni traccia nella playlist, estrai le info aggiuntive e aggiungile al dataset
        for playlist in spotify_db["playlists"]:
            tracks_augmented = []
            # @TODO: rendi asincrono
            for track in playlist["tracks"]:
                track_uri = track["track_uri"].split(":")[2]
                artist_uri = track["artist_uri"].split(":")[2]
                artist_dict = {}
                
                # get features
                try:
                    track_features = await s.get_track(track_uri)
                except Exception as e:
                    print("Error while getting track features " + str(e))
                    continue

                try:
                    artist_features = await s.get_artist(artist_uri)
                except Exception as e:
                    print("Error while getting artist features " + str(e))
                    continue

                # extract artist features
                artist_dict["genres"] = artist_features.genres
                artist_dict["popularity"] = artist_features.popularity
                artist_dict["followers"] = artist_features.followers

                # estria ed assegna le track features
                track["popularity"] = track_features.popularity
                track["explicit"] = track_features.explicit
                track["release_date"] = track_features.album.release_date
                track["artist"] = artist_dict
                tracks_augmented.append(track)

            playlists_augmented.append(playlist)
            

#======================================================================================
#=========================================MAIN=========================================
#======================================================================================
if __name__ == "__main__":

    dataset_path = "../dataset/data/"
    augmented_dataset_dir = "../dataset/augmented_data/"
    log_dir = "../logs/"

    # Per ogni file runno in maniera asincrona l'augment delle playlist al suo interno
    for file in os.listdir(dataset_path):
        start_time = time.time()
        asyncio.run(augment(dataset_path, file))
        end_time = time.time()
        with open(log_dir + file + ".log", "w") as log:
            log.write("Augmentation completed for file " + file + " in " + end_time-start_time + " seconds\n")