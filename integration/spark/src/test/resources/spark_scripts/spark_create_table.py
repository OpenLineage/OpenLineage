import os

os.makedirs("/tmp/create_test", exist_ok=True)

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName("Open Lineage Integration Create Table") \
    .config("spark.sql.warehouse.dir", "file:/tmp/create_test/") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel('info')

spark.sql("CREATE TABLE create_table_test (a string, b string)")