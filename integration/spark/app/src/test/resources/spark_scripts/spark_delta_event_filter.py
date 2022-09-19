# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName("Open Lineage Integration Delta") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .getOrCreate()

spark.sql("CREATE TABLE t1 (a long, b long) USING delta LOCATION '/tmp/t1'")                   # 2 OL events expected
df = spark.createDataFrame([
    {'a': 1 },
    {'a': 2 }
])
df.write.saveAsTable('temp')
spark.sql("CREATE TABLE t2 USING delta LOCATION '/tmp/t2' AS SELECT * FROM temp WHERE a > 1") # 2 OL events expected
spark.sql("ALTER TABLE t2 ADD COLUMNS (b long)")                                              # 2 OL events expected
spark.sql("INSERT INTO t1 VALUES (3,4)")                                                      # 2 OL events expected
spark.sql("ALTER TABLE t1 ADD COLUMNS (c long)")                                              # 2 OL events expected