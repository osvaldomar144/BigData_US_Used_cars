#!/usr/bin/env python3

import sys
import json

# Legge riga per riga dallo standard input (stdin)
for line in sys.stdin:
    # Rimuove eventuali spazi e newline finali
    line = line.strip()
    try:
        # Tenta di spacchettare i campi in base al separatore TAB
        city, year, tier, daysonmarket, description = line.split("\t")

        # Verifica che il campo 'daysonmarket' sia un numero
        if daysonmarket.isnumeric():
            word_count = {}

            # Conta la frequenza di ogni parola nella descrizione
            for word in description.split():
                if word not in word_count:
                    word_count[word] = 1
                else:
                    word_count[word] += 1

            # Stampa i campi originali più il dizionario delle parole (in formato JSON)
            print("\t".join([
                city,
                year,
                tier,
                daysonmarket,
                json.dumps(word_count, ensure_ascii=False)  # preserva caratteri speciali
            ]))
    except:
        # In caso di errore (es. riga malformata), passa alla prossima riga
        continue