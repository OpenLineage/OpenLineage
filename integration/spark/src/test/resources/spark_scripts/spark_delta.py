# SPDX-License-Identifier: Apache-2.0

import os
from pyspark.sql import SparkSession

os.makedirs("/tmp/delta", exist_ok=True)

spark = SparkSession.builder \
    .master("local") \
    .appName("Open Lineage Integration Delta") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("packages", "io.delta:delta-core_2.12:1.0.0") \
    .getOrCreate()
spark.sparkContext.setLogLevel('info')

df = spark.createDataFrame([
    {'a': 1, 'b': 2},
    {'a': 3, 'b': 4}
])
df.createOrReplaceTempView('temp')

spark.sql("CREATE TABLE tbl USING delta LOCATION '/tmp/delta' AS SELECT * FROM temp")
