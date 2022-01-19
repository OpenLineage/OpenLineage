# SPDX-License-Identifier: Apache-2.0

import os

from pyspark.sql import SparkSession

os.makedirs("/tmp/v2_replace_table", exist_ok=True)

spark = SparkSession.builder \
    .master("local") \
    .appName("Open Lineage Integration Delta") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("packages", "io.delta:delta-core_2.12:1.0.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel('info')

spark.sql("CREATE TABLE tbl (a string, b string) USING delta LOCATION '/tmp/v2_replace_table'")
spark.sql("REPLACE TABLE tbl (c string, d string) USING delta LOCATION '/tmp/v2_replace_table'")