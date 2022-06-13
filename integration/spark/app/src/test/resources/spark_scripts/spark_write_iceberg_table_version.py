import os
from pyspark.sql import SparkSession

os.makedirs("/tmp/v2", exist_ok=True)

spark = SparkSession.builder \
    .master("local") \
    .appName("Open Lineage Integration Iceberg") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "/tmp/write_iceberg_table_version") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .getOrCreate()

spark.sql("CREATE TABLE local.db.table (a int, b int) USING iceberg")
spark.sql("INSERT INTO local.db.table VALUES (1, 2)")
