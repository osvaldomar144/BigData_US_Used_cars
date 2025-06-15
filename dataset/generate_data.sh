#!/bin/bash

if [ "$1" != "local[*]" ] && [ "$1" != "yarn" ]; then
    echo "Error: Invalid master argument. Use 'local[*]' or 'yarn'."
    exit 1
fi

if [ "$1" == "local[*]" ]; then
    SPARK_CMD="$SPARK_HOME/bin/spark-submit"
else
    SPARK_CMD="spark-submit"
fi

if [ ! -f "data/data_cleaned.csv" ]; then
    $SPARK_CMD \
        --master $1 \
        clean_data.py

    cd data
    rm -rf used_cars_data.csv
    mv part-*.csv data_cleaned.csv
    cd ..
fi

hdfs dfs -mkdir -p /user/$USER/data
hdfs dfs -put data/data_cleaned.csv /user/$USER/data
#hdfs dfs -mv /user/$USER/data/part-*.csv data/data_cleaned.csv

$SPARK_CMD \
    --master $1 \
    generate_portions.py \
    --fractions "0.01 0.2 0.5 0.7"