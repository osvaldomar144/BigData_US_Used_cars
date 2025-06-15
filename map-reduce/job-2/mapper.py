#!/usr/bin/env python3

import sys

# Funzione per classificare il prezzo in una fascia
def price_category(price: str) -> str:
    price = float(price)

    if price >= 50000:
        return "alto"
    elif price >= 20000:
        return "medio"
    else:
        return "basso"

# Legge ogni riga in input
for line in sys.stdin:
    try:
        # Estrae i campi della riga CSV
        city, daysonmarket, description, *_ , price, year = line.strip().split(",")

        # Conversioni di tipo: giorni sul mercato e prezzo
        daysonmarket = int(daysonmarket)
        price_tag = price_category(price)

        # Rimuove spazi multipli e ricompone la descrizione come una stringa unica
        description_clean = ",".join(description.split())

        # Stampa il risultato in formato chiave/valore, separato da TAB
        # Chiave:    city::year::fascia_prezzo
        # Valore:    1::daysonmarket::[descrizione]
        print(f"{city}::{year}::{price_tag}\t1::{daysonmarket}::[{description_clean}]")

    except ValueError:
        # Salta righe malformate (ad esempio con campi non convertibili)
        continue
