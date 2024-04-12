# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os

from pyspark.sql import SparkSession

os.makedirs("/tmp/emit_test", exist_ok=True)

spark = (
    SparkSession.builder.master("local")
    .appName("Open Lineage Integration Emit Metrics")
    .config("spark.sql.warehouse.dir", "file:/tmp/emit_test/")
    .config("spark.openlineage.debugFacet", "enabled")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("info")

spark.sql("CREATE TABLE create_table_emit_test (a string, b string)")
