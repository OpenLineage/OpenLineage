import os
from pyspark.sql import SparkSession

os.makedirs("/tmp/delta", exist_ok=True)

spark = SparkSession.builder \
    .master("local") \
    .appName("Open Lineage Integration Delta") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("packages", "io.delta:delta-core_2.12:1.0.0") \
    .config("spark.sql.warehouse.dir", "file:/tmp/delta/") \
    .getOrCreate()
spark.sparkContext.setLogLevel('info')

df = spark.createDataFrame([
    {'a': 1, 'b': 2},
    {'a': 3, 'b': 4}
])
df.write.saveAsTable('temp')

spark.sql("CREATE TABLE tbl USING delta LOCATION '/tmp/delta/tbl' AS SELECT * FROM temp")
