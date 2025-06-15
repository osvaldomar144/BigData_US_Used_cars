#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType, DoubleType, StringType
import os
import re

def clean_value(val):
    if val is None:
        return val
    val = re.sub(r'[^a-zA-Z0-9_.,\- ]', '', val)
    val = val.replace(',', ' ')
    return val

ROOT_DIR = os.getenv("ROOT_DIR")
USER = os.getenv("USER")
spark = SparkSession.builder.appName("data-cleaning").getOrCreate()

clean_udf = udf(clean_value, StringType())

df = spark.read \
    .option("header", True) \
    .csv(f"file://{ROOT_DIR}/dataset/data/used_cars_data.csv") \
    .select(
        "city",
        "daysonmarket",
        "description",
        "engine_displacement",
        "horsepower",
        "make_name", 
        "model_name",
        "price",
        "year"
    )

string_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, StringType)]
for col_name in string_columns:
    df = df.withColumn(col_name, clean_udf(col(col_name)))

df = df.filter(
    col("price").rlike("^[0-9]+(\.[0-9]+)?$") &
    col("year").rlike("^[0-9]{4}$")
).withColumn("price", col("price").cast(DoubleType())) \
 .withColumn("year", col("year").cast(IntegerType()))

df.coalesce(1).write \
    .option("header", False) \
    .mode("append") \
    .csv(f"file://{ROOT_DIR}/dataset/data")

'''
df.coalesce(1).write \
    .option("header", False) \
    .mode("append") \
    .csv(f"/user/{USER}/data")
'''

spark.stop()