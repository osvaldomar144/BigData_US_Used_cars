#!/bin/bash

mkdir data
curl -L -o \
    data/us-used-cars-dataset.zip \
    https://www.kaggle.com/api/v1/datasets/download/ananaymital/us-used-cars-dataset

cd data
unzip us-used-cars-dataset.zip
rm -rf us-used-cars-dataset.zip
cd ..