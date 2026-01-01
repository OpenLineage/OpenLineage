# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os
import time

from pyspark.sql import SparkSession

os.makedirs("/tmp/warehouse", exist_ok=True)


spark = (
    SparkSession.builder.master("local")
    .appName("Open Lineage Integration Hive")
    .config("spark.sql.catalogImplementation", "hive")
    .config("spark.sql.hive.metastore.uris", "http://metastore")
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("info")

spark.sql("CREATE TABLE IF NOT EXISTS test (key INT, value STRING) USING hive")
time.sleep(1)
