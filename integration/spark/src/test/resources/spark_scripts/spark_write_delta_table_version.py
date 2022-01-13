import os
from pyspark.sql import SparkSession

os.makedirs("/tmp/v2", exist_ok=True)

spark = SparkSession.builder \
    .master("local") \
    .appName("Open Lineage Integration Delta") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("packages", "io.delta:delta-core_2.12:1.0.0") \
    .getOrCreate()

spark.sql("CREATE TABLE versioned_table (a int, b int) USING delta LOCATION '/tmp/write_delta_table_version'")
spark.sql("INSERT INTO versioned_table VALUES (1, 2)")
