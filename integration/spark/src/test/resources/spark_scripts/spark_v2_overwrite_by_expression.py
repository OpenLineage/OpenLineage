import os

from pyspark.sql import SparkSession

os.makedirs("/tmp/v2_overwrite", exist_ok=True)

spark = SparkSession.builder \
    .master("local") \
    .appName("Open Lineage Integration Delta") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("packages", "io.delta:delta-core_2.12:1.0.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel('info')

df = spark.createDataFrame([
    {'id': 1},
    {'id': 2},
    {'id': 3}
])
df.createOrReplaceTempView('mydata')

spark.sql("CREATE TABLE testtable( id INT ) USING DELTA LOCATION '/tmp/v2_overwrite'")
spark.sql("INSERT OVERWRITE testtable SELECT * FROM mydata")


