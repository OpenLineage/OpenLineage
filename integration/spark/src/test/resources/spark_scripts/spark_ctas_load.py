# SPDX-License-Identifier: Apache-2.0

import os
import time

os.makedirs("/tmp/ctas_load", exist_ok=True)

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName("Open Lineage Integration CTAS Load") \
    .config("spark.sql.warehouse.dir", "/tmp/ctas_load") \
    .enableHiveSupport() \
    .getOrCreate()
spark.sparkContext.setLogLevel('info')

df = spark.createDataFrame([
    {'a': 1, 'b': 2},
    {'a': 3, 'b': 4}
])

df.write.saveAsTable('temp')

spark.sql("CREATE TABLE tbl1 USING hive LOCATION '/tmp/ctas_load/tbl1' AS SELECT a, b FROM temp")

spark.sql(f"LOAD DATA LOCAL INPATH '/test_data/test_data.csv' INTO TABLE tbl1")
