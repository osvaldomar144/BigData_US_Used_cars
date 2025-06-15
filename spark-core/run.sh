#!/bin/bash

# remove output folder if already exists
hdfs dfs -rm -r -f /user/$USER/spark-core/$1

if [ "$3" == "local[*]" ]; then
    SPARK_CMD="$SPARK_HOME/bin/spark-submit"
else
    SPARK_CMD="spark-submit"
fi

$SPARK_CMD \
    --master $3 \
    $1.py \
    -input /user/$USER/data/$2.csv \
    -output /user/$USER/spark-core/$1