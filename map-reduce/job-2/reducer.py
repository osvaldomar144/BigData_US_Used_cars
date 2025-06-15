import sys

def emit_result(
    key: str,
    tot_car: int,
    tot_daysonmarket: int,
    word_count: dict
) -> None:
    """
    Stampa il risultato aggregato per una chiave.

    Args:
        key: Chiave corrente (es. combinazione di anno e produttore).
        tot_car: Numero totale di auto per la chiave.
        tot_daysonmarket: Somma totale dei giorni sul mercato per la chiave.
        word_count: Dizionario delle parole nelle descrizioni con la loro frequenza.
    """
    # Ordina il dizionario delle parole per frequenza decrescente
    word_dict = dict(sorted(word_count.items(), key=lambda item: item[1], reverse=True))

    # Estrai le prime 3 parole più frequenti
    top_3_words = list(map(lambda x: x[0], word_dict.items()))[:3]

    # Calcola la media dei giorni sul mercato, con gestione divisione per zero
    avg_days = round(tot_daysonmarket / tot_car, 2) if tot_car else 0

    # Sostituisce i separatori personalizzati con tabulazioni per l'output finale
    key = key.replace("::", "\t")

    # Stampa finale della riga aggregata
    print(f"{key}\t{tot_car}\t{avg_days}\t{top_3_words}")

# Inizializzazione variabili di aggregazione
word_count = {}
current_key = None
tot_car = 0
tot_daysonmarket = 0

# Lettura riga per riga dallo standard input
for line in sys.stdin:
    # Estrae chiave e valore (separati da TAB)
    key, value = line.strip().split("\t")

    # Estrae i campi dal valore (formato: count::days::[descrizione])
    counter, daysonmarket, description = value.split("::", 2)

    # Quando cambia la chiave, emette il risultato aggregato precedente
    if key != current_key:
        if current_key is not None:
            emit_result(current_key, tot_car, tot_daysonmarket, word_count)
        word_count = {}
        tot_car = 0
        tot_daysonmarket = 0

    # Aggiorna i contatori
    tot_car += int(counter)
    tot_daysonmarket += int(daysonmarket)

    # Rimuove le parentesi quadre dalla descrizione, poi divide in parole
    description = description.strip("[]")
    words = description.split(",") if description else []

    # Conta la frequenza di ciascuna parola
    for word in words:
        word = word.strip()
        if word:  # evita parole vuote
            word_count[word] = word_count.get(word, 0) + 1

    current_key = key