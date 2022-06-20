# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os

from pyspark.sql import SparkSession

os.makedirs("/tmp/v2_overwrite", exist_ok=True)

spark = SparkSession.builder \
    .master("local") \
    .appName("Open Lineage Integration V2 Commands") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "/tmp/v2_overwrite") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .getOrCreate()
spark.sparkContext.setLogLevel('info')

df = spark.createDataFrame([
    {'a': 1, 'b': 2, 'c': 3},
    {'a': 4, 'b': 5, 'c': 6}
])
df.createOrReplaceTempView('temp')
spark.sql("CREATE TABLE local.db.source USING iceberg AS SELECT * FROM temp")

spark.sql("CREATE TABLE local.db.tbl (a long, b  long) PARTITIONED BY (c long)")
spark.sql("INSERT INTO local.db.tbl PARTITION (c=1) VALUES (2, 3)")
spark.sql("INSERT OVERWRITE TABLE local.db.tbl PARTITION(c) SELECT * FROM local.db.source")


