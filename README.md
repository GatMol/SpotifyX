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

## Esecuzione

1. Lanciare hdfs `start-all.sh` ed eventualmente `hdfs dfsadmin -safemode leave`
2. Lanciare mongodb ```$MONGO_HOME/bin/mongod --dbpath $MONGO_HOME/data --logpath $MONGO_HOME/logs/mongo.log```
3. Lanciare lo streaming_consumer tramite [run_streaming_consumer.sh](./consumer/run_streaming_consumer.sh)
4. Lanciare i producer, kafka e zookeeper tramite `docker compose up`
5. Lanciare hdfs_consumer tramite [run_hdfs_consumer.sh](./consumer/run_hdfs_consumer.sh)

## Esecuzione su AWS
Di seguito i passi per eseguire il progetto su AWS, ma prima sono elencati i requisiti necessari

### Requisiti
- Creare un account MongoDB con un database chiamato `spotifyxcluster`

Ora sei pronto a lanciare il progetto in cloud usando AWS. Segui i seguenti passi:
1. Lanciare un'istanza EC2 con Kafka e Zookeeper installati (seguendo la [guida](https://medium.com/@khasnobis.sanjit890/installing-apache-kafka-in-aws-ec2-instance-own-your-kafka-server-for-0-0992-per-hour-32cd78e7cf27))
2. Modifica `server.properties` cambiando `advertised.listeners=PLAINTEXT://<public ip ec2 instance with kafka (and zookeeper) running on it>:9092`
3. Crea il topic `playlist-topic` su kafka
4. Configura e lancia un'istanza AWS EMR con Spark
5. Aggiorna i file di configurazione come suggerito sotto 
6. Copia le cartelle [consumer](./consumer/) e [analytics](./analytics/) nell'istanza EMR
7. Lancia i consumer appena caricati su EMR
8. Lancia i [producer](./producers/) in locale

## File di configurazione
- Crea un file `analytics/mongoConfig.py` nella root del progetto con le seguenti variabili:
```python
mongo_uri = "<mongo connection string uri>"
```
- Crea un file `./awsConfig.py` nella root del progetto con le seguenti variabili:
```python
kafka_broker_IP="<public ip ec2 instance with kafka (and zookeeper) running on it>"
kafka_port="<port used in security group TCP custom>"
```
