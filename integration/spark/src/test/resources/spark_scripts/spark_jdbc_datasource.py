import os
from pyspark.sql import SparkSession

os.makedirs("/tmp/tbl", exist_ok=True)

spark = SparkSession.builder \
    .master("local") \
    .appName("Open Lineage Integration JDBC") \
    .getOrCreate()
spark.sparkContext.setLogLevel('info')

df = spark.createDataFrame([
    {'a': 1, 'b': 2},
    {'a': 3, 'b': 4}
])

df.write \
    .mode("overwrite") \
    .format("jdbc") \
    .option("url", "jdbc:derby:memory:;databaseName=/tmp/tbl;create=true") \
    .option("dbtable", "tbl") \
    .save()
