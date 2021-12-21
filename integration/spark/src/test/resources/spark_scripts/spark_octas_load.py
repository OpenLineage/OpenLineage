import os

os.makedirs("/tmp/ctas_load", exist_ok=True)

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName("Open Lineage Integration OCTAS Load") \
    .config("spark.sql.warehouse.dir", "/tmp/ctas_load") \
    .enableHiveSupport() \
    .getOrCreate()
spark.sparkContext.setLogLevel('info')

df = spark.createDataFrame([
    {'a': 1, 'b': 2},
    {'a': 3, 'b': 4}
])

df.createOrReplaceTempView('temp')

spark.sql("CREATE TABLE tbl2 STORED AS PARQUET LOCATION '/tmp/ctas_load/tbl2' AS SELECT a, b FROM temp")
