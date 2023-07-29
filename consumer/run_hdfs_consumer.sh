CURRENT_FILE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# hdfs consumer
python $CURRENT_FILE_DIR/hdfs_consumer.py