#!/usr/bin/env python3

import sys


def emit_result(
    key: str,
    tot_car: int,
    tot_daysonmarket: int,
    word_count: dict
) -> None:
    word_dict = dict(sorted(word_count.items(), key=lambda item: item[1], reverse=True))
    top_3_words = list(map(lambda x: x[0], word_dict.items()))[:3]

    if tot_car == 0:
        avg_days = 0
    else:
        avg_days = round(tot_daysonmarket / tot_car, 2)

    key = key.replace("::","\t")

    print(f"{key}\t{tot_car}\t{avg_days}\t{top_3_words}")

word_count = {}
current_key = None
tot_car = 0
tot_daysonmarket = 0

for line in sys.stdin:
    key, value = line.strip().split("\t")
    counter, daysonmarket, description = value.split("::", 2)

    if key != current_key:
        emit_result(key, tot_car, tot_daysonmarket, word_count)
        word_count = {}
        tot_car = 0
        tot_daysonmarket = 0

    tot_car += int(counter)
    tot_daysonmarket += int(daysonmarket)

    description = description.strip("[]")
    words = description.split(",") if description else []

    for word in words:
        if word not in word_count:
            word_count[word] = 1
        else:
            word_count[word] += 1

    current_key = key