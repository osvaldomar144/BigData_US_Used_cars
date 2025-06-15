#!/usr/bin/env python3

import sys
import json

for line in sys.stdin:
    line = line.strip()
    try:
        city, year, tier, daysonmarket, description = line.split("\t")

        if daysonmarket.isnumeric():
            word_count = {}
            for word in description.split():
                if word not in word_count:
                    word_count[word] = 1
                else:
                    word_count[word] += 1

            print("\t".join([city, year, tier, daysonmarket, json.dumps(word_count, ensure_ascii=False)]))
    except:
        continue

