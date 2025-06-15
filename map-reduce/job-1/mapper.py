#!/usr/bin/env python3

import sys

import sys

# Itera su ogni riga letta dallo standard input (tipicamente usato in Hadoop Streaming)
for line in sys.stdin:
    # Rimuove spazi bianchi finali e divide i campi CSV
    fields = line.strip().split(",")

    # Verifica che la riga abbia almeno 9 colonne prima di accedere agli indici
    if len(fields) < 9:
        continue  # ignora righe malformate

    # Estrae i campi di interesse
    make_name = fields[5]     # Marca del veicolo
    model_name = fields[6]    # Modello del veicolo
    price = fields[7]         # Prezzo del veicolo
    year = fields[8]          # Anno del veicolo

    # Stampa i dati: make, model, 1 (conteggio), prezzo, anno
    print(f"{make_name}\t{model_name}\t1\t{price}\t{year}")