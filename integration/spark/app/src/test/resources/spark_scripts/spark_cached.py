# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os
import time

os.makedirs("/tmp/caching", exist_ok=True)

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName("Open Lineage In Memory Relation") \
    .config("spark.sql.warehouse.dir", "/tmp/caching") \
    .enableHiveSupport() \
    .getOrCreate()
spark.sparkContext.setLogLevel('info')

spark.createDataFrame([
    {'a': 1, 'b': 2},
    {'a': 3, 'b': 4}
]).write.option("compression", "none").saveAsTable('temp')

cached = spark.read.table("temp").cache()       # create cached dataset
cached.count()                                  # call an action on cached dataset
cached\
    .select('a')\
    .write\
    .option("compression", "none")\
    .saveAsTable('target')                      # target table lineage should be aware of temp table