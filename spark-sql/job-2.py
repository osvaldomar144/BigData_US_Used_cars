from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as spark_round, concat_ws, array, split
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType, IntegerType
import argparse

# --- Parsing argomenti CLI ---
parser = argparse.ArgumentParser()
parser.add_argument("-input", type=str, help="Path to input file")
parser.add_argument("-output", type=str, help="Path to output folder")
args = parser.parse_args()

# --- Funzione per elaborare una riga dell'RDD ---
def process_row(row):
    city = row['city']
    year = row['year']
    fascia = row['fascia']
    numero_macchine = row['numero_macchine']
    avg_daysonmarket = row['avg_daysonmarket']
    descriptions:list[str] = row['descriptions_list']

    # Conta le parole (solo parole alfabetiche, lunghezza > 0)
    word_counts = {}
    for word in descriptions:
        if len(word) > 0 and word.isalpha():
            word_counts[word] = word_counts.get(word, 0) + 1

    # Ordina le parole per frequenza decrescente e prendi le prime 3
    sorted_words = sorted(word_counts.items(), key=lambda x: (x[1]), reverse=True)
    top_3 = list(map(lambda x: x[0], sorted_words[:3]))

    return (city, year, fascia, numero_macchine, avg_daysonmarket, top_3)

# --- Inizializzazione Spark ---
spark = SparkSession.builder \
    .config("spark.driver.host", "localhost") \
    .appName("spark-sql#job-2") \
    .getOrCreate()

# --- Definizione schema per il CSV ---
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

# --- Lettura e selezione delle colonne necessarie ---
df = spark.read \
    .csv(args.input, schema=schema) \
    .select("city", "daysonmarket", "description", "price", "year")

# --- Filtro per valori validi in daysonmarket (devono essere numerici) ---
df = df.filter(
    col("daysonmarket").rlike("^[0-9]+$")
).createOrReplaceTempView("dataset")

# --- Query SQL per aggregare i dati ---
query = """
SELECT 
    city, 
    year, 
    CASE 
        WHEN price < 20000 THEN 'basso'
        WHEN price BETWEEN 20000 AND 50000 THEN 'medio'
        ELSE 'alto'
    END AS fascia,
    COUNT(*) AS numero_macchine,
    AVG(daysonmarket) AS avg_daysonmarket,
    COLLECT_LIST(description) AS descriptions_list
FROM dataset
GROUP BY city, year, 
    CASE 
        WHEN price < 20000 THEN 'basso'
        WHEN price BETWEEN 20000 AND 50000 THEN 'medio'
        ELSE 'alto'
    END
"""
# Esegue la query e restituisce un DataFrame
final_report = spark.sql(query) 

# --- Arrotonda e concatena le descrizioni in una singola stringa ---
final_report = final_report \
    .withColumn("avg_daysonmarket", spark_round(col("avg_daysonmarket"), 2)) \
    .withColumn("descriptions_list", array(concat_ws(" ", col("descriptions_list"))))

# --- Divide la stringa unica in parole separate, creando un array ---
final_report = final_report.withColumn("descriptions_list", split(col("descriptions_list")[0], " "))

# --- Conversione del DataFrame in RDD per usare la funzione custom ---
df_rdd = final_report.select("city", "year", "fascia", "numero_macchine", "avg_daysonmarket", "descriptions_list").rdd

# --- Applica la funzione `process_row` per contare parole e trovare le top 3 ---
processed_rdd = df_rdd.map(process_row)

# --- Definizione dello schema per il risultato finale ---
schema = StructType([
    StructField("city", StringType(), False),
    StructField("year", StringType(), False),
    StructField("fascia", StringType(), False),
    StructField("num_macchine", IntegerType(), False),
    StructField("avg_daysonmarket", DoubleType(), False),
    StructField("top_3_words", ArrayType(StringType()), False)
])

# --- Creazione del DataFrame finale e formattazione delle parole chiave ---
final_result_df = spark.createDataFrame(processed_rdd, schema)
final_result_df = final_result_df.withColumn("top_3_words", concat_ws(",", col("top_3_words")))

# --- Visualizzazione output ---
final_result_df.show(n = 10)

# --- Chiusura della sessione Spark ---
spark.stop()