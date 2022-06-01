# SPDX-License-Identifier: Apache-2.0

import os
import time

os.makedirs("/tmp/ctas_load", exist_ok=True)

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName("Open Lineage Integration Truncate Table") \
    .config("spark.sql.warehouse.dir", "file:/tmp/truncate_test/") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel('info')

spark.sql("CREATE TABLE truncate_table_test (a string, b string)")

spark.sql("TRUNCATE TABLE truncate_table_test")

time.sleep(3)