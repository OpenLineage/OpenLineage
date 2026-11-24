# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os
import sys
import time

from pyspark.sql import SparkSession

if os.path.exists("/tmp/events.log"):
    os.remove("/tmp/events.log")

# suffix for names and paths, e.g. 16_4_scala2_13, passed by DatabricksEnvironment.runScript
runtime_version = sys.argv[1]

spark = SparkSession.builder.appName("narrow_transformation").getOrCreate()

data = [(1, "a"), (2, "b"), (3, "c")]
rdd = spark.sparkContext.parallelize(data)
df = rdd.toDF(["id", "value"])

df = df.withColumn("id_plus_one", df["id"] + 1)

df.write.mode("overwrite").parquet("data/path/to/output/narrow_transformation_{}/".format(runtime_version))

time.sleep(3)

event_file = "dbfs:/databricks/openlineage/events_{}.log".format(runtime_version)
dbutils.fs.rm(event_file, True)
dbutils.fs.cp("file:/tmp/events.log", event_file)
