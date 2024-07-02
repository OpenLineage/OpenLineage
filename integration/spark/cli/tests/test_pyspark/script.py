# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import time

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("CLI test application").enableHiveSupport().getOrCreate()
spark.sql("CREATE TABLE IF NOT EXISTS t1 (a INT, b STRING);")
spark.sql("INSERT INTO t1 VALUES (1,2),(3,4);")
spark.sql("CREATE TABLE IF NOT EXISTS t2 AS SELECT * FROM t1;")

time.sleep(3)
