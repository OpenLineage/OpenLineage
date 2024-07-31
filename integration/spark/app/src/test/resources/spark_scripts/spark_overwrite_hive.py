# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os
import time

from pyspark.sql import SparkSession

os.makedirs("/tmp/overwrite", exist_ok=True)


spark = (
    SparkSession.builder.master("local")
    .appName("Open Lineage Integration Overwrite Hive")
    .config("spark.sql.warehouse.dir", "/tmp/overwrite")
    .config("spark.openlineage.facets.disabled", "spark_unknown;spark.logicalPlan")
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("info")

spark.sql("CREATE TABLE IF NOT EXISTS test (key INT, value STRING) USING hive")

spark.sql("INSERT OVERWRITE DIRECTORY '/tmp/overwrite/table' USING hive VALUES (1, 'a'), (2, 'b'), (3, 'c')")
result = spark.sql("SELECT count(*) from test")

time.sleep(3)
