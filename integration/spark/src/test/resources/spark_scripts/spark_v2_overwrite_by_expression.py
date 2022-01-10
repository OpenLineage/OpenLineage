import os
import time

from pyspark.sql import SparkSession

os.makedirs("/tmp/v2_overwrite", exist_ok=True)

spark = SparkSession.builder \
    .master("local") \
    .appName("Open Lineage Integration V2 Commands") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "/tmp/v2_overwrite") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("packages", "org.apache.iceberg:iceberg-spark3-runtime:0.12.1") \
    .getOrCreate()
spark.sparkContext.setLogLevel('info')

df = spark.createDataFrame([
    {'a': 1, 'b': 2},
    {'a': 3, 'b': 4}
])
df.createOrReplaceTempView('temp')

spark.sql("CREATE TABLE local.db.tbl USING iceberg AS SELECT * FROM temp")
spark.sql("INSERT OVERWRITE local.db.tbl VALUES (5,6),(7,8)")


