# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from pyspark.sql import SparkSession
from time import sleep

# Parameter provisioned by the templating system
BUCKET_NAME = "{{bucketName}}"
OUTPUT_PREFIX = "{{outputPrefix}}"
NAMESPACE = "{{namespace}}"

spark = (
    SparkSession.builder.config("spark.openlineage.namespace", NAMESPACE).appName("emr_example").getOrCreate()
)

people_df = spark.createDataFrame(
    [
        (1, "John", "Smith", 49),
        (2, "Mary", "Brown", 12),
        (3, "Tom", "White", 51),
        (4, "Bruce", "Willis", 18),
        (5, "Jason", "Mane", 22),
    ],
    ["id", "first_name", "last_name", "age"],
)

people_df.write.mode("overwrite").format("parquet").option(
    "path", f"s3://{BUCKET_NAME}/{OUTPUT_PREFIX}/emr_test_script/test"
).saveAsTable("destination_table")

sleep(3)
