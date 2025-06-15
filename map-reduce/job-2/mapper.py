#!/usr/bin/env python3

import sys

def price_category(price: str):
    price = float(price)

    if price >= 50000:
        return "alto"
    elif price >= 20000 and price < 50000:
        return "medio"
    else:
        return "basso"

for line in sys.stdin:
    city, daysonmarket, description, _, _, _, _, price, year = line.strip().split(",")

    try:
        daysonmarket = int(daysonmarket)
        price_tag = price_category(price)
        description = ",".join(description.split())

        print(f"{city}::{year}::{price_tag}\t1::{daysonmarket}::[{description}]")
    except ValueError:
        continue