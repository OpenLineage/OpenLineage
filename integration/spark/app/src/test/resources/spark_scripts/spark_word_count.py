# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("Open Lineage Integration Word Count").getOrCreate()
spark.sparkContext.setLogLevel('info')

df = spark.read.text("/test_data/data.txt")

agg = df.groupBy("value").count()
agg.write.mode("overwrite").csv("/tmp/test_data/test_output/")