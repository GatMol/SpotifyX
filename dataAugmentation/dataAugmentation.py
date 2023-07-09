import os
import json
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import threading
import os

# Creazione Spotify
spotify = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials())

def get_track_features(track):
    """
    funzione che restituisce la track passata come parametro e alcune informazioni associate
    
    @param id spotify della traccia 
    @return dizionario con chiave la traccia e valore le informazioni estratte associate
    """

    track_uri = track["track_uri"]   # id traccia
    artist_uri = track["artist_uri"] # id artista
    artist_dict = {}                 # dizionario per le informazioni associate all'artista
    
    # Get artista
    try:
        artist_features = spotify.artist(artist_uri)
        print("Getting artist features for: " + artist_uri)
        # se l'artista è presente nel catalogo spotify allora non è stato cancellato da esso
        artist_dict["deleted"] = False
        # Per artista: genere, popolarità, totale dei followers
        artist_dict["genres"] = artist_features["genres"]
        artist_dict["popularity"] = artist_features["popularity"]
        artist_dict["followers"] = artist_features["followers"]["total"]
    except:
        # @TODO: gestisci la possibilità che sia tornato un errore HTTP
        # se c'è stato un problema???
        # dovrei capire se non c'è o c'è stato qualche altro errore 
        artist_dict["deleted"] = True 
        print("Artist not found: " + artist_uri)

    # Get traccia
    try:
        # @TODO: si puo togliere .track e metterlo fuori così da catturare l'eccezione e leggere lo status code
        track_features = spotify.track(track_uri)
        # Per traccia: Artista_dict, popolarità, esplicito, rilascio album
        print("Getting track features for: " + track_uri)
        track["artist"] = artist_dict
        track["popularity"] = track_features["popularity"]
        track["explicit"] = track_features["explicit"]
        track["release_date"] = track_features["album"]["release_date"]
        # se tutto va bene allora è stato trovato nel catalogo (quindi non è stato cancellato)
        track["deleted"] = False
    except:
        # @TODO: come sopra
        track["deleted"] = True
        print("Track not found: " + track_uri)
    
    return track

def get_playlist_tracks(playlist):
    """
    restituisce le canzoni di una playlist con le relative informazioni

    @param id playlist di cui si voglione arricchire le informazioni circa le canzoni
    @return tracce della playlist con tutte le informazioni estratte
    """
    
    tracks = [] # dizionario key=traccia, value=informazioni associate
    
    # per ogni traccia della playlist estrai le informazioni associate
    for track in playlist["tracks"]:
        tracks.append(get_track_features(track))

    return tracks

def get_track_features_multiprocessing(file_path, file):
    """
    arricchisce le informazioni relative alle playlist del file

    @TODO: Cambia argomenti
    @param 
    """

    # apri il file in questione che dovrai aggiornare
    df = json.load(open(file_path))
    playlists = []
    
    for playlist in df["playlists"]:
        # arricchisci le informazioni
        playlist["tracks"] = get_playlist_tracks(playlist)
        playlists.append(playlist)

    # aggiorna il dato
    df["playlists"] = playlists
    # salvo il dato augmentato
    json.dump(df, open(augmented_dataset_dir + file, "w"))

def get_playlist_for_files(start, end, dataset_dir):
    """
    prende le playlist dallo #start file all'#end file ed arricchisce le informazioni delle playlsit associate

    @param indice primo file
    @param indice ultimo file
    @param path del dataset da cui prendere i file con le playlist
    """

    for file in os.listdir(dataset_dir)[start:end]:
        print("Processing file: " + file)
        # per ognuno di questi file estrae le informazioni 
        get_track_features_multiprocessing(dataset_dir + file, file)
    
#======================================================================================
#=========================================MAIN=========================================
#======================================================================================
if __name__ == "__main__":

    dataset_dir = "../dataset/data/"
    augmented_dataset_dir = "../dataset/augmented_data/"

    # numero di core disponibili (-1 per lasciarlo all'applicazione)    
    num_cores = os.cpu_count() - 1
    threads = []

    print("Starting data augmentation with " + str(num_cores) + " cores...")

    # dividi i file per numero di core
    file_per_core = len(os.listdir(dataset_dir)) // num_cores
    
    for i in range(num_cores - 1):
        # Per ogni file ho più playlist e creo tanti thread quanti possibili che arricchiscano le informazioni circa le playlist
        t = threading.Thread(target=get_playlist_for_files, args=(i * file_per_core, (i + 1) * file_per_core, dataset_dir))
        threads.append(t)
        t.start()

    # Potrebbero dover mancare alcuni file, quindi il restante lo assegno ad un altro thread
    # @TODO: si potrebber fare meglio e bilanciare meglio il carico
    t = threading.Thread(target=get_playlist_for_files, args=((num_cores - 1) * file_per_core, len(os.listdir(dataset_dir)), dataset_dir))
    threads.append(t)
    t.start()

    for thread in threads:
        thread.join()

    print("Done!")