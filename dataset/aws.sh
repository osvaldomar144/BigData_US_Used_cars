#!/bin/bash


cd data
hdfs dfs -mkdir -p /user/hadoop/data

mv data-1.0%.csv data-1.csv
hdfs dfs -put data-1.csv /user/hadoop/data
hdfs dfs -mv data/data-1.csv data/data-1.0%.csv

mv data-20.0%.csv data-20.csv
hdfs dfs -put data-20.csv /user/hadoop/data
hdfs dfs -mv data/data-20.csv data/data-20.0%.csv

mv data-50.0%.csv data-50.csv
hdfs dfs -put data-50.csv /user/hadoop/data
hdfs dfs -mv data/data-50.csv data/data-50.0%.csv

mv data-70.0%.csv data-70.csv
hdfs dfs -put data-70.csv /user/hadoop/data
hdfs dfs -mv data/data-70.csv data/data-70.0%.csv

hdfs dfs -put data_cleaned.csv /user/hadoop/data