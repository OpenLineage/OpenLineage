# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os
import time

from pyspark.sql import SparkSession

os.makedirs("/tmp/ctas_load", exist_ok=True)


spark = (
    SparkSession.builder.master("local")
    .appName("Open Lineage Integration Truncate Table")
    .config("spark.sql.warehouse.dir", "file:/tmp/truncate_test/")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("info")

spark.sql("CREATE TABLE truncate_table_test (a string, b string)")

spark.sql("TRUNCATE TABLE truncate_table_test")

time.sleep(3)
