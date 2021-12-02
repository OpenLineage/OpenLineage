import os

os.makedirs("/tmp/ctas_load", exist_ok=True)

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName("Open Lineage Integration Alter Table") \
    .config("spark.sql.warehouse.dir", "file:/tmp/alter_test/") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel('info')

spark.sql("CREATE TABLE alter_table_test (a string, b string)")
spark.sql("ALTER TABLE alter_table_test ADD COLUMNS (c string, d string)")
spark.sql("ALTER TABLE alter_table_test RENAME TO alter_table_test_new")
