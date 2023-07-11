import os
import json
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import threading
import os
import time

def augment_playlists(idx_start, num_playlist_to_process, playlists, filename, nth_thread):
    """
    prende un range di playlist e ne arricchisce le informazioni

    @param indice prima playlist
    @param numero playlist da processare
    @param path del file da cui prendere le playlist

    @return le playlist arricchite
    """
    
    # se il thread è pari uso le credenziali di Gatto
    if nth_thread % 2 == 0:
        spotify = spotify_gatto

    # altrimenti quelle di moli
    else:
        spotify = spotify_moli

    for playlist in playlists[idx_start+1:idx_start + num_playlist_to_process - 1]:
        print("Working on playlist " + str(playlist["name"]))
        # Arricchisco le informazioni circa le tracce della playlist
        for track in playlist["tracks"]:
            track_uri = track["track_uri"].split(":")[2]   # id traccia
            artist_uri = track["artist_uri"].split(":")[2] # id artista
            artist_dict = {}                               # dizionario per le informazioni associate all'artista
            
            # Get artista
            try:
                artist_features = spotify.artist(artist_uri)
                # se l'artista è presente nel catalogo spotify allora non è stato cancellato da esso
                artist_dict["deleted"] = False
                # Per artista: genere, popolarità, totale dei followers
                artist_dict["genres"] = artist_features["genres"]
                artist_dict["popularity"] = artist_features["popularity"]
                artist_dict["followers"] = artist_features["followers"]["total"]
            except Exception as e:
                print("Error while getting artist features " + str(e))
                # @TODO: gestisci la possibilità che sia tornato un errore HTTP
                artist_dict["deleted"] = True 

            # Get traccia
            try:
                track_features = spotify.track(track_uri)
                # Per traccia: Artista_dict, popolarità, esplicito, rilascio album
                track["artist"] = artist_dict
                track["popularity"] = track_features["popularity"]
                track["explicit"] = track_features["explicit"]
                track["release_date"] = track_features["album"]["release_date"]
                # se tutto va bene allora è stato trovato nel catalogo (quindi non è stato cancellato)
                track["deleted"] = False
            except Exception as e:
                # @TODO: come sopra
                print("Error while getting track features " + str(e))
                track["deleted"] = True
            time.sleep(0.1)

        # salvo le playlist arricchite per avere degli snapshot intermedi relativi a file non ancora completamente processati
        with open(tmp_aug_playlist_path + filename + "/" + playlist["name"] + ".json", "a") as f:
            json.dump(playlist, f)
        time.sleep(10)

    return playlists
    
#======================================================================================
#=========================================MAIN=========================================
#======================================================================================
if __name__ == "__main__":

    dataset_dir = "./dataset/data/"
    augmented_dataset_dir = "./dataset/augmented_data/"
    tmp_aug_playlist_path = "./dataset/tmp_aug_playlists/"
    log_dir = "./logs/"
    
    # Creazione Spotify
    spotify_gatto = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials(
        client_id="",
        client_secret="",
    ))
    spotify_moli = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials(
        client_id="",
        client_secret="",
    ))

    # numero di core disponibili (-1 per lasciarlo all'applicazione)    
    num_cores = os.cpu_count() - 1
    # num_cores = 1
    threads = []

    with open(log_dir + "augmentation.log", "w") as log_file:
        # @TODO: cerca di capire il bottleneck!!!
        # in ogni file, divido playlist per numero di core
        # N.B. il bottleneck probabilmente è dovuto alle TANTE playlists presenti in ogni file
        # (sono più le playlist per file che le tracce in ogni playlist)
        for filename in os.listdir(dataset_dir):
            # creo la cartella cui nome è il nome del file
            os.mkdir(tmp_aug_playlist_path + filename)

            print("Processing file " + filename)
            # apri e leggi il file in questione
            with open(dataset_dir + filename, "r") as f:
                f_json = json.load(f)
                
                # prendi le playlist
                playlists = f_json["playlists"]
                # @TODO rimuovi le playlist già processate 
                num_playlists = len(playlists) # quante playlist ci sono nel file
                
            start_time = time.time()
            t = time.localtime()
            yy = t.tm_year
            gg = t.tm_mday
            mm = t.tm_mon
            hh = t.tm_hour
            min = t.tm_min
            log_file.write("========Processing file " + filename + "=============\n")
            log_file.write(f"Started at {yy}-{mm}-{gg} {hh}:{min}\n")

            # Calcola quante playlist assegno ad ogni core e il resto eventuale
            playlists_per_core = num_playlists // num_cores
            reminder_playlist_per_core = num_playlists % num_cores
            # Spartisco il numero di playlist per core
            num_playlists_per_core = [playlists_per_core + 1 if i < reminder_playlist_per_core else playlists_per_core for i in range(num_cores)]
            log_file.write("Number of cores: " + str(num_cores) + "\n")
            log_file.write("Number of playlists per core: " + str(num_playlists_per_core) + "\n")

            # per ogni core arricchisci le info delle playlist
            for i in range(num_cores):
                # se il thread è pari allora scrivo che le richieste le faccio con l'account di gatto
                if i % 2 == 0:
                    log_file.write("Thread " + str(i) + " will use gatto's account\n")
                else:
                    log_file.write("Thread " + str(i) + " will use moli's account\n")
                
                log_file.write("Starting thread " + str(i) + "\n")
                t = threading.Thread(target=augment_playlists, args=(sum(num_playlists_per_core[:i])-1, num_playlists_per_core[i], playlists, filename, i))
                
                threads.append(t)
                
                log_file.write("Started thread " + str(i) + "\n")
                t.start()

            log_file.write("Waiting for threads to finish\n")
            for thread in threads:
                thread.join()

            end_time = time.time()
            t = time.localtime()
            yy = t.tm_year
            gg = t.tm_mday
            mm = t.tm_mon
            hh = t.tm_hour
            min = t.tm_min
            log_file.write(f"finished at {yy}-{mm}-{gg} {hh}:{min}\n")
            log_file.write("File processed in " + str(end_time - start_time) + " seconds\n")
            
            # aggiorna ed arricchisci i dati
            with open(augmented_dataset_dir + filename, "w") as f:
                json.dump(f_json, f)

            log_file.write("===================DONE========================\n")