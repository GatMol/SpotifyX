#!/bin/bash

if [ $# -ne 3 ]; then
    echo "Usage: $0 <analysis_file> <input_dir> <output_collection_name>"
    exit 1
fi

# take arguments from input
analysis_file=$1
input_dir=$2
output_collection_name=$3

spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.0  --master yarn $analysis_file --input $input_dir --output $output_collection_name