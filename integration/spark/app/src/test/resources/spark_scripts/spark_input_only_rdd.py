# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("Open Lineage Integration Input Only RDD").getOrCreate()

# prepare some data and store in Parquet
df = spark.createDataFrame([{"a": "x", "b": "z"}, {"a": "y", "b": "z"}])

df.write.option("compression", "none").parquet("/tmp/input_rdd")

# read RDDs out of the parquet file
rdd = spark.read.parquet("/tmp/input_rdd").rdd

# repartition action should not write output dataset
rdd.map(lambda x: (x["a"] + x["b"],)).repartition(2)
