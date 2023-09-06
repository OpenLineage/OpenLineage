# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from pyspark.sql import SparkSession

import os, time;

if os.path.exists("/tmp/events.log"):
  os.remove("/tmp/events.log")

spark = (
    SparkSession
        .builder
        .appName("wide_transformation")
        .getOrCreate()
)

data = [(1,"a"),(2,"b"),(3,"c")]
rdd = spark.sparkContext.parallelize(data)
df = rdd.toDF(["id", "value"])

(
    df
      .groupBy("id")
      .count()
      .write
      .mode("overwrite")
      .parquet("data/output/wide_transformation/result/")
)

time.sleep(3)

dbutils.fs.cp(
  "file:/tmp/events.log",
  "dbfs:/databricks/openlineage/events.log"
)