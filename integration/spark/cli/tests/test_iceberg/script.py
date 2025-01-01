# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os
import time

from pyspark.sql import SparkSession

os.makedirs("/tmp/iceberg", exist_ok=True)  # noqa: S108

spark = (
    SparkSession.builder.master("local")
    .appName("CLI test application")
    .enableHiveSupport()
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.spark_catalog.type", "hadoop")
    .config("spark.sql.catalog.spark_catalog.warehouse", "/tmp/iceberg")  # noqa: S108
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)

spark.sql("CREATE TABLE create_table_test (a string, b string) USING iceberg;")

time.sleep(3)
