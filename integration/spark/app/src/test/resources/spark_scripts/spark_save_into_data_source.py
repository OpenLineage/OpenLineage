# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os

os.makedirs("/tmp/save_into_data_source", exist_ok=True)

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.master("local")
    .appName("Open Lineage")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .getOrCreate()
)

data = [(8, "bat"), (64, "mouse"), (-27, "horse")]

df = spark.sparkContext.parallelize(data).toDF(["a", "b"])
df.write.mode("overwrite").format("delta").save(
    "/tmp/save_into_data_source_target/"
)
