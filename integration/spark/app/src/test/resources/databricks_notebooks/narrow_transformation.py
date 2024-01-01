# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os
import time

from pyspark.sql import SparkSession

if os.path.exists("/tmp/events.log"):
    os.remove("/tmp/events.log")

spark = SparkSession.builder.appName("narrow_transformation").getOrCreate()

data = [(1, "a"), (2, "b"), (3, "c")]
rdd = spark.sparkContext.parallelize(data)
df = rdd.toDF(["id", "value"])

df = df.withColumn("id_plus_one", df["id"] + 1)

(df.write.mode("overwrite").parquet("data/path/to/output/narrow_transformation/"))

time.sleep(3)

dbutils.fs.cp("file:/tmp/events.log", "dbfs:/databricks/openlineage/events.log")
