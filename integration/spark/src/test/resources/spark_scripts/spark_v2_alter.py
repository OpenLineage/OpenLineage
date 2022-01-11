import os, time
from pyspark.sql import SparkSession

os.makedirs("/tmp/v2_alter", exist_ok=True)

spark = SparkSession.builder \
    .master("local") \
    .appName("Open Lineage Integration V2 Commands") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "/tmp/v2_alter") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .getOrCreate()
spark.sparkContext.setLogLevel('info')

spark.sql("CREATE TABLE local.db.alter_table_test (a string, b string) USING iceberg")
spark.sql("INSERT INTO local.db.alter_table_test VALUES ('a', 'b')")
spark.sql("ALTER TABLE local.db.alter_table_test RENAME COLUMN b TO c")
