#!/usr/bin/env python3

import sys

# Dizionario per raggruppare i dati per make e model
group = {}

# Legge ciascuna riga dallo standard input (tipicamente da un mapper Hadoop)
for line in sys.stdin:
    # Estrae i campi dalla riga tab-delimited
    make_name, model_name, count, price, year = line.strip().split("\t")

    # Converte i campi in tipi appropriati
    count = int(count)
    price = float(price)
    year = int(year)

    # Usa make e model come chiave per il raggruppamento
    key = f"{make_name}\t{model_name}"

    # Inizializza l'aggregato se è la prima volta che si vede questa chiave
    if key not in group:
        group[key] = {
            "num_car": 0,
            "price_sum": 0.0,
            "price_min": float("inf"),
            "price_max": float("-inf"),
            "years": set()
        }

    # Aggiorna i valori aggregati
    group[key]["num_car"] += count
    group[key]["price_sum"] += price
    group[key]["price_min"] = min(price, group[key]["price_min"])
    group[key]["price_max"] = max(price, group[key]["price_max"])
    group[key]["years"].add(year)

# Stampa i risultati finali per ciascuna chiave
for key, stats in group.items():
    make_name, model_name = key.split("\t")
    num_cars = stats["num_car"]
    avg_value = round(stats["price_sum"] / num_cars, 2)
    min_value = stats["price_min"]
    max_value = stats["price_max"]
    years = sorted(stats["years"])  # Ordina gli anni

    # Output finale tab-delimited
    print(f"{make_name}\t{model_name}\t{num_cars}\t{min_value}\t{max_value}\t{avg_value}\t{years}")
