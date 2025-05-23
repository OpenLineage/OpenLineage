# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0


from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("Smart Debug Facet Test").getOrCreate()

spark.sql("SHOW TABLES")
