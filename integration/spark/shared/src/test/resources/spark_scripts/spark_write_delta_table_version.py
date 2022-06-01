import os

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName("Open Lineage Integration Delta") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .getOrCreate()

spark.sql("CREATE TABLE versioned_table (a long, b long) USING delta LOCATION '/tmp/versioned_table'")      # VERSION 1
spark.sql("ALTER TABLE versioned_table ADD COLUMNS (c long)")                                               # VERSION 2

df = spark.createDataFrame([
    {'a': 1 },
    {'a': 2 }
]).repartition(1)
df.createOrReplaceTempView('temp')
spark.sql("CREATE TABLE versioned_input_table USING delta LOCATION '/tmp/versioned_input_table' AS SELECT * FROM temp") # VERSION 1
spark.sql("ALTER TABLE versioned_input_table ADD COLUMNS (b long)")                                                     # VERSION 2
spark.sql("INSERT INTO versioned_input_table VALUES (3,4)")
spark.sql("ALTER TABLE versioned_input_table ADD COLUMNS (c long)")                                                     # VERSION 3

spark.sql("INSERT INTO versioned_table SELECT * FROM versioned_input_table")
