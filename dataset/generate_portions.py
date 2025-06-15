from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType, StringType, StructField, StructType
import os
import argparse

ROOT_DIR = os.getenv("ROOT_DIR")
USER = os.getenv("USER")

parser = argparse.ArgumentParser()
parser.add_argument("--fractions", type=str, help="Fractions to split the dataset")
args = parser.parse_args()
fractions = list(map(lambda x: float(x), args.fractions.split()))

spark = SparkSession.builder.appName("generate-portions").getOrCreate()

schema = StructType([
    StructField(name="city", dataType=StringType(), nullable=True),
    StructField(name="daysonmarket", dataType=IntegerType(), nullable=True),
    StructField(name="description", dataType=StringType(), nullable=True),
    StructField(name="engine_displacement", dataType=DoubleType(), nullable=True),
    StructField(name="horsepower", dataType=DoubleType(), nullable=True),
    StructField(name="make_name", dataType=StringType(), nullable=True),
    StructField(name="model_name", dataType=StringType(), nullable=True),
    StructField(name="price", dataType=DoubleType(), nullable=True),
    StructField(name="year", dataType=IntegerType(), nullable=True)
])

for fraction in fractions:
    df = spark.read \
        .csv(f"/user/{USER}/data/data_cleaned.csv", schema=schema) \
        .sample(withReplacement=False, fraction=fraction)
    df.coalesce(1).write.csv(f"data/data-{fraction * 100}%")
    os.system(f"hdfs dfs -mv data/data-{fraction * 100}%/part-*.csv data/data-{fraction * 100}%.csv")
    #os.system(f"hdfs dfs -get data/data-{fraction * 100}%.csv data")
    os.system(f"hdfs dfs -rm -r -f data/data-{fraction * 100}%")

spark.stop()