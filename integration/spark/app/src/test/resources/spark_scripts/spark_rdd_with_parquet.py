# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName("Open Lineage Integration RDD with Parquet") \
    .getOrCreate()

# prepare some data and store in Parquet
df = spark.createDataFrame([
    {'a': 'x', 'b': 'z'},
    {'a': 'y', 'b': 'z'}
]);

df.write.option("compression", "none").parquet("/tmp/rdd_a")
df.write.option("compression", "none").parquet("/tmp/rdd_b")

# read RDDs out of the parquet file
rdd_a = spark.read.parquet("/tmp/rdd_a").rdd
rdd_b = spark.read.parquet("/tmp/rdd_b").rdd

# make a union, some map on RDDs, convert to dataframe and save as parquet
rdd_a \
  .union(rdd_b) \
  .map(lambda x: (x['a'] + x['b'],)) \
  .toDF() \
  .write.parquet("/tmp/rdd_c")