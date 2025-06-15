DROP TABLE IF EXISTS used_cars;

CREATE TABLE used_cars (
    city STRING,
    daysonmarket INT,
    description STRING,
    engine_displacement FLOAT,
    horsepower FLOAT,
    make_name STRING,
    model_name STRING,
    price FLOAT,
    year INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

LOAD DATA INPATH '${hivevar:input_path}' OVERWRITE INTO TABLE used_cars;
ADD FILE count_word.py;
ADD FILE top_3_words.py;

CREATE TABLE IF NOT EXISTS transform_1 AS
    SELECT 
        city, 
        year,
        CASE 
            WHEN price < 20000 THEN 'basso'
            WHEN price BETWEEN 20000 AND 50000 THEN 'medio'
            ELSE 'alto'
        END AS tier,
        daysonmarket,
        description
    FROM used_cars;

CREATE TABLE IF NOT EXISTS transform_2 AS
    SELECT
        TRANSFORM(
            city, year, tier, daysonmarket, description
        ) USING 'python3 count_word.py'
        AS city, year, tier, daysonmarket, word_count
    FROM transform_1;

CREATE TABLE IF NOT EXISTS transform_3 AS
    SELECT
        city,
        year,
        tier,
        COUNT(*) as num_cars,
        AVG(daysonmarket) as avg_daysonmarket,
        COLLECT_SET(word_count) as word_count_list
    FROM transform_2
    GROUP BY city, year, tier;

SELECT *
FROM transform_3
LIMIT 10;

DROP TABLE used_cars;
DROP TABLE transform_1;
DROP TABLE transform_2;
DROP TABLE transform_3;