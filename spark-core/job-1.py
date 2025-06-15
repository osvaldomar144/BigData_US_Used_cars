#!/usr/bin/env python3

from pyspark.sql import SparkSession
import argparse

# Parser degli argomenti della CLI
parser = argparse.ArgumentParser()
parser.add_argument("-input", type=str, help="Path to input file")
parser.add_argument("-output", type=str, help="Path to output folder")
args = parser.parse_args()

# Inizializzazione della sessione Spark
spark = SparkSession.builder \
    .appName("spark-core#job-1") \
    .getOrCreate()

# Caricamento del file di testo come RDD
rdd = spark.sparkContext.textFile(args.input)

# Pipeline di trasformazione:
# 1. Split delle righe CSV
# 2. Mappatura in chiave (make_name, model_name) e valori aggregabili
# 3. Riduzione per chiave per aggregare i dati
# 4. Calcolo delle statistiche finali (media, ordinamento anni, ecc.)

processed_RDD = (
    rdd
    .map(lambda line: line.split(","))  # Divide ogni riga in campi
    .filter(lambda x: len(x) >= 9)  # evita errori se la riga ha campi mancanti
    .map(lambda x: (
        (x[5], x[6]),  # Chiave: (make_name, model_name)
        (
            1,                      # Contatore (1 veicolo)
            float(x[7]),            # Prezzo corrente (usato per somma media)
            float(x[7]),            # Prezzo minimo iniziale
            float(x[7]),            # Prezzo massimo iniziale
            {int(x[8])}             # Anno in un set per evitare duplicati
        )
    ))
    .reduceByKey(lambda v1, v2: (  # Aggregazione
        v1[0] + v2[0],              # Somma veicoli
        v1[1] + v2[1],              # Somma prezzi
        min(v1[2], v2[2]),          # Prezzo minimo
        max(v1[3], v2[3]),          # Prezzo massimo
        v1[4] | v2[4]               # Unione degli anni (set)
    ))
    .map(lambda x: (                # Riformattazione finale
        x[0][0],                    # make_name
        x[0][1],                    # model_name
        x[1][0],                    # num_cars
        x[1][2],                    # min_price
        x[1][3],                    # max_price
        round(x[1][1] / x[1][0], 2),# avg_price
        sorted(list(x[1][4]))       # Lista ordinata di anni
    ))
)

# Stampa le prime 10 righe dell'RDD trasformato
for line in processed_RDD.take(10):
    print(line)

# Chiude la sessione Spark
spark.stop()