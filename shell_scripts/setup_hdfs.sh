#!/bin/bash


echo "Setting up HDFS..."

# Create HDFS directories
echo "Creating HDFS directories..."
hdfs dfs -mkdir /user
hdfs dfs -mkdir /user/spotifyx
hdfs dfs -mkdir /user/spotifyx/input
hdfs dfs -mkdir /user/spotifyx/output

src_dir=$(dirname $0)
batch_dir=$src_dir/../dataset/batch_data

echo "Copying batch data to HDFS..."
# for each file in batch_data, copy to hdfs
for file in $batch_dir/*; do
    hdfs dfs -put $file /user/spotifyx/input
done