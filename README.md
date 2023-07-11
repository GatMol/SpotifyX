# SpotifyX

## Dataset
- dataset batch: [spotify_million](https://www.kaggle.com/datasets/himanshuwagh/spotify-million)
- dati in streaming provenienti da [spotiPy](https://spotipy.readthedocs.io/en/2.22.1/?highlight=playlist#examples)

## Tecnologie utilizzate
- [Kafka Python](https://kafka-python.readthedocs.io/en/master/index.html)
- [Structured Streaming Spark](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [PySpark](https://spark.apache.org/docs/latest/api/python/getting_started/index.html)

## Struttura del progetto
- [data augmentation](./dataAugmentation), contiene tutto il necessario per arricchire i dati con informazioni aggiuntive delle canzoni presenti nelle playlist del dataset
- [dataset](./dataset/), 
  - [data](./dataset/data), contiene tutti i file di [spotify_million](https://www.kaggle.com/datasets/himanshuwagh/spotify-million)
  - [augmented_data](./dataset/augmented_data), contiene i file (di cui sopra), arricchiti con informazioni aggiuntive
  - [tmp_aug_playlists](./dataset/tmp_aug_playlists/), contiene i file di cui sopra, contenenti tanti file quante sono le playlists già processate