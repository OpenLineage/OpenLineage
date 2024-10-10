# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

from time import sleep

# Parameter provisioned by the templating system
BUCKET_NAME = "{{bucketName}}"
OUTPUT_PREFIX = "{{outputPrefix}}"
DATABASE_NAME = "{{databaseName}}"
SOURCE_TABLE_NAME = "{{sourceTableName}}"
DESTINATION_TABLE_NAME = "{{destinationTableName}}"

conf = SparkConf().setAll([
    ("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
])

sc = SparkContext.getOrCreate(conf)

spark = (
    SparkSession.builder.appName("Glue symlink implicit account id").getOrCreate()
)

spark.sql("CREATE DATABASE IF NOT EXISTS glue_symlink_test")
people_df = spark.createDataFrame(
    [
        (1, "John", "Smith", 49),
        (2, "Mary", "Brown", 12),
        (3, "Tom", "White", 51),
        (4, "Bruce", "Willis", 18),
        (5, "Jason", "Mane", 22),
    ],
    ["id", "first_name", "last_name", "age"],
).writeTo(f"{DATABASE_NAME}.{SOURCE_TABLE_NAME}").createOrReplace()

people_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .option("path", f"s3://{BUCKET_NAME}/{OUTPUT_PREFIX}/destination_people") \
    .saveAsTable("olt85.{destination_people}")

people_df.write.mode("overwrite").format("parquet").option(
    "path", f"s3://{BUCKET_NAME}/{OUTPUT_PREFIX}/emr_test_script/test"
).saveAsTable(f"{DATABASE_NAME}.{DESTINATION_TABLE_NAME}")

sleep(3)
