# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os

from pyspark.sql import SparkSession

os.makedirs("/tmp/create_test", exist_ok=True)

spark = (
    SparkSession.builder.master("local")
    .appName("Smart Debug Facet Test")
    .config("spark.sql.warehouse.dir", "file:/tmp/create_test/")
    .enableHiveSupport()
    .getOrCreate()
)

# This should have an output dataset -> no smart debug facet
spark.sql("CREATE TABLE test_table (a string, b string)")

# This should have input dataset but no output dataset -> should smart debug facet
spark.sql("SHOW TABLES")

# This should outputs -> no smart debug facet
spark.sql("INSERT INTO test_table VALUES ('1', '2')")
