# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os
from pyspark.sql import SparkSession

os.makedirs("/tmp/v2", exist_ok=True)

spark = SparkSession.builder \
    .master("local") \
    .appName("Open Lineage Integration V2 Commands") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "/tmp/v2") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .getOrCreate()
spark.sparkContext.setLogLevel('info')

df = spark.createDataFrame([
    {'a': 1, 'b': 2},
    {'a': 3, 'b': 4}
])
df.createOrReplaceTempView('temp')
spark.sql("CREATE TABLE local.db.source1 USING iceberg AS SELECT * FROM temp")
spark.sql("CREATE TABLE local.db.source2 USING iceberg AS SELECT * FROM temp")

spark.sql("CREATE TABLE local.db.target USING iceberg AS (SELECT * FROM local.db.source1 UNION SELECT * FROM local.db.source2)")
