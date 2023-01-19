# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os
import time

os.makedirs("/tmp/failure_test", exist_ok=True)

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName("Open Lineage Integration Failure Test") \
    .config("spark.sql.warehouse.dir", "file:/tmp/failure_test/") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel('info')
# let emit open lineage emit START event
spark.sql("CREATE TABLE failure_test (a string)")

# wait for OpenLineage to fail
time.sleep(3)

# make sure Spark is still fine
spark.sql("INSERT INTO failure_test VALUES ('something')")
if (spark.read.table("failure_test").count() > 0):
    print("Spark is fine!")