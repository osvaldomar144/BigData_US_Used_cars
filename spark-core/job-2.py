#!/usr/bin/env python3

from pyspark.sql import SparkSession
import argparse

# Parsing degli argomenti da linea di comando
parser = argparse.ArgumentParser()
parser.add_argument("-input", type=str, help="Path to input file")
parser.add_argument("-output", type=str, help="Path to output folder")
args = parser.parse_args()

# Funzione per classificare le auto in fasce di prezzo
def group_by_price(x):
    if x > 50000:
        return "alto"
    elif x > 20000:
        return "medio"
    else:
        return "basso"

# Funzione per estrarre le 3 parole più frequenti da una descrizione
def top_3_words(description: str):
    word_counts = {}
    for word in description.split():
        if word.isalpha():  # Considera solo parole fatte di sole lettere
            word_counts[word] = word_counts.get(word, 0) + 1
    # Ordina le parole per frequenza decrescente
    sorted_words = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)
    return [word for word, _ in sorted_words[:3]]

# Inizializza la sessione Spark
spark = SparkSession.builder \
    .appName("spark-core#job-2") \
    .getOrCreate()

# Legge il file CSV come RDD di righe di testo
rdd = spark.sparkContext.textFile(args.input)

# Pipeline di elaborazione:
processed_RDD = (
    rdd
    .map(lambda line: line.split(","))  # Split dei campi CSV
    .filter(lambda x: len(x) >= 9 and x[1].isdigit())  # Filtra righe malformate o con daysonmarket non numerico
    .map(lambda x: (
        x[0],                      # city
        int(x[1]),                 # daysonmarket
        x[2],                      # description
        group_by_price(float(x[7])),  # fascia prezzo calcolata
        int(x[8])                  # year
    ))
    .map(lambda x: (
        (x[0], x[4], x[3]),        # chiave: (city, year, fascia)
        (1, x[1], x[2])            # valore: (conteggio, daysonmarket, descrizione)
    ))
    .reduceByKey(lambda a, b: (
        a[0] + b[0],               # somma dei veicoli
        a[1] + b[1],               # somma daysonmarket
        a[2] + " " + b[2]          # concatena le descrizioni
    ))
    .map(lambda x: (
        x[0][0],                   # city
        x[0][1],                   # year
        x[0][2],                   # fascia prezzo
        x[1][0],                   # numero veicoli
        round(x[1][1] / x[1][0], 2),  # media daysonmarket
        top_3_words(x[1][2])       # top 3 parole dalla descrizione
    ))
)

# Stampa i primi 10 risultati
for line in processed_RDD.take(10):
    print(line)

# Chiude la sessione Spark
spark.stop()