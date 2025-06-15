#!/usr/bin/env python3

'''
SELECT
    TRANSFORM(
        city, year, tier, num_cars, avg_daysonmarket, word_count_list
    ) USING 'python3 top_3_words.py'
    AS city, year, tier, num_cars, avg_daysonmarket, descriptions_list
FROM transform_2
LIMIT 10;
'''

import sys
import json

# Process each line from stdin
for line in sys.stdin:
    city, year, tier, num_cars, avg_daysonmarket, word_count_list = line.strip().split("\t")

    word_counter = {}
    for word_count in word_count_list:
        obj = json.loads(word_count)