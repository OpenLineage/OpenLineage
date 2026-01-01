# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os
import time
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.master("local")
    .appName("Open Lineage Integration single input RDD with input statistics")
    .getOrCreate()
)

input_dir = "/tmp/rdd_input"
os.makedirs(input_dir, exist_ok=True)
lines = ["hello world", "hello pyspark rdd", "spark rdd transforms are fun", "hello again world"]
with open(os.path.join(input_dir, "sample.txt"), "w", encoding="utf-8") as f:
    f.write("\n".join(lines))


def fake_map(x):
    time.sleep(0.1)  # catching active job won't work for super fast RDD operations
    return x


os.makedirs("/tmp/rdd_output", exist_ok=True)  # emulate writing to a partitioned location
(spark.sparkContext.textFile("/tmp/rdd_input").map(fake_map).saveAsTextFile("/tmp/rdd_output/20251030"))
