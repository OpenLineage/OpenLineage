# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os
import time

from pyspark.sql import SparkSession

os.makedirs("/tmp/alter_test", exist_ok=True)

spark = (
    SparkSession.builder.master("local")
    .appName("Open Lineage Integration Alter Table")
    .config("spark.sql.warehouse.dir", "file:/tmp/alter_test/")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("info")

spark.sql("CREATE TABLE alter_table_test (a string, b string)")
time.sleep(1)
spark.sql("ALTER TABLE alter_table_test ADD COLUMNS (c string, d string)")
time.sleep(1)
spark.sql("ALTER TABLE alter_table_test RENAME TO alter_table_test_new")
