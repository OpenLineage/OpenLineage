import os

os.makedirs("/tmp/warehouse", exist_ok=True)

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName("Open Lineage Integration Delta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel('info')

data = spark.range(0, 5)
data.write.format("delta").save("/tmp/warehouse/delta-table")

