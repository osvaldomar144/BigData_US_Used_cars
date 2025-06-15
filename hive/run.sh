#!/bin/bash

rm -rf metastore_db
schematool -dbType derby -initSchema

hive \
    -hivevar input_path=/user/$USER/data/data-70.0%.csv \
    --f $1.hql