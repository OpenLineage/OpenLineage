# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from pyspark.sql import SparkSession

from time import sleep

# Parameter provisioned by the templating system
BUCKET_NAME = "{{bucketName}}"
OUTPUT_PREFIX = "{{outputPrefix}}"
DATABASE_NAME = "{{databaseName}}"
SOURCE_TABLE_NAME = "{{sourceTableName}}"
DESTINATION_TABLE_NAME = "{{destinationTableName}}"

spark = (
    SparkSession.builder.appName("Glue symlink implicit account id")
    .config(
        "hive.metastore.client.factory.class",
        "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
    )
    .config("spark.sql.catalogImplementation", "hive")
    .getOrCreate()
)

spark.sql(f"DROP DATABASE IF EXISTS {DATABASE_NAME} CASCADE")

spark.sql(f"CREATE DATABASE {DATABASE_NAME} LOCATION 's3://{BUCKET_NAME}/{OUTPUT_PREFIX}/{DATABASE_NAME}/'")

source_table = f"{DATABASE_NAME}.{SOURCE_TABLE_NAME}"
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
people_df.writeTo(source_table).create()

destination_table = f"{DATABASE_NAME}.{DESTINATION_TABLE_NAME}"
spark.sql(f"""CREATE TABLE {destination_table} AS (SELECT * FROM {source_table})""")

(
    spark.sql(f"SELECT * FROM {source_table}")
    .write.mode("overwrite")
    .format("parquet")
    .saveAsTable(f"{DATABASE_NAME}.{DESTINATION_TABLE_NAME}")
)

spark.sql(f"DROP DATABASE IF EXISTS {DATABASE_NAME} CASCADE")

sleep(3)
