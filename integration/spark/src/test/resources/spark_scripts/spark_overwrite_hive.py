# SPDX-License-Identifier: Apache-2.0

import os

os.makedirs("/tmp/overwrite", exist_ok=True)

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName("Open Lineage Integration Overwrite Hive") \
    .config("spark.sql.warehouse.dir", "/tmp/overwrite") \
    .enableHiveSupport() \
    .getOrCreate()
spark.sparkContext.setLogLevel('info')

spark.sql("CREATE TABLE IF NOT EXISTS test (key INT, value STRING) USING hive")

spark.sql(f"INSERT OVERWRITE DIRECTORY '/tmp/overwrite/table' USING hive VALUES (1, 'a'), (2, 'b'), (3, 'c')")
result = spark.sql("SELECT count(*) from test")