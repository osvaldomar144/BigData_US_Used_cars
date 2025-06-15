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

SELECT 
    make_name,
    model_name,
    COUNT(*) as num_cars,
    MIN(price) as min_price,
    MAX(price) as max_price,
    AVG(price) as avg_price,
    COLLECT_SET(year) as years_list
FROM used_cars
GROUP BY make_name, model_name
LIMIT 10;

DROP TABLE used_cars;