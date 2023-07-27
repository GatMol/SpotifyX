CURRENT_FILE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 --master yarn $CURRENT_FILE_DIR/streaming_consumer.py
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 --master local $CURRENT_FILE_DIR/streaming_consumer.py