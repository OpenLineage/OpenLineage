# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os

from pyspark.sql import SparkSession

os.makedirs("/tmp/warehouse", exist_ok=True)


spark = (
    SparkSession.builder.master("local")
    .appName("Open Lineage Integration HadoopFS relation")
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("info")

spark.sql("CREATE TABLE IF NOT EXISTS test (key INT, value STRING) USING parquet")
spark.sql("CREATE TABLE IF NOT EXISTS target (key INT, value STRING) USING parquet")
spark.sql("INSERT INTO test VALUES (1, 'a'), (2, 'b'), (3, 'c')")

spark.sql("INSERT INTO target SELECT * from test WHERE value > 1")
