import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

spotify = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials())

try:
    t = spotify.artist("6eUKZXaKkcviH0Ku9w2n3V")
    print(t)
except Exception as e: 
    print(e)