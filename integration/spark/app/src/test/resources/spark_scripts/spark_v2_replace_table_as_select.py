# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os

from pyspark.sql import SparkSession

os.makedirs("/tmp/v2_replace_table_as_select", exist_ok=True)

spark = SparkSession.builder \
    .master("local") \
    .appName("Open Lineage Integration V2 Commands") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "/tmp/v2_replace_table_as_select") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .getOrCreate()
spark.sparkContext.setLogLevel('info')

df = spark.createDataFrame([
    {'a': 1, 'b': 2},
    {'a': 4, 'b': 5}
])
df.write.saveAsTable('local.db.temp')

spark.sql("CREATE TABLE local.db.tbl USING iceberg")
spark.sql("REPLACE TABLE local.db.tbl USING iceberg AS SELECT * FROM local.db.temp")
