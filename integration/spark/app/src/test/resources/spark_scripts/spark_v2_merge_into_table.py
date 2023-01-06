# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os

from pyspark.sql import SparkSession

os.makedirs("/tmp/v2_merge", exist_ok=True)

spark = SparkSession.builder \
    .master("local") \
    .appName("Open Lineage Integration V2 Commands") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "/tmp/v2_merge") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .getOrCreate()
spark.sparkContext.setLogLevel('info')

spark.sparkContext.setLogLevel('info')

spark.sql("CREATE TABLE local.db.events (event_id long, last_updated_at long) USING iceberg")
spark.sql("CREATE TABLE local.db.updates (event_id long, updated_at long) USING iceberg")

spark.sql("INSERT INTO local.db.events VALUES (1, 1641290276);")
spark.sql("INSERT INTO local.db.updates VALUES (1, 1641290277);")
spark.sql("INSERT INTO local.db.updates VALUES (2, 1641290277);")


spark.sql("""
MERGE INTO local.db.events USING local.db.updates 
ON local.db.events.event_id = local.db.updates.event_id
WHEN MATCHED THEN
UPDATE SET local.db.events.last_updated_at = local.db.updates.updated_at
WHEN NOT MATCHED
THEN INSERT (event_id, last_updated_at) VALUES (event_id, updated_at)
""")



