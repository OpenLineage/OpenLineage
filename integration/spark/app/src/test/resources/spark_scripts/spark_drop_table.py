# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os

from pyspark.sql import SparkSession

os.makedirs("/tmp/ctas_load", exist_ok=True)


spark = (
    SparkSession.builder.master("local")
    .appName("Open Lineage Integration Drop Table")
    .config("spark.sql.warehouse.dir", "file:/tmp/drop_test/")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("info")

spark.sql("CREATE TABLE drop_table_test (a string, b string)")

spark.sql("DROP TABLE drop_table_test")
