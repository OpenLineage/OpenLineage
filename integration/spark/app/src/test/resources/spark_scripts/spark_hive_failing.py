# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os

from pyspark.sql import SparkSession

os.makedirs("/tmp/warehouse", exist_ok=True)


spark = SparkSession.builder.master("local").appName("FailingApplication").enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("info")

spark.sql("INSERT INTO NON_EXISTING_TABLE SELECT * from ANOTHER_NON_EXISTING_TABLE WHERE value > 1")
