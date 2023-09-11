# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os, time;

dbutils.fs.rm("dbfs:/mnt/openlineage/test/t1", True)
dbutils.fs.mkdirs("dbfs:/mnt/openlineage/test/t1")

if os.path.exists("/tmp/events.log"):
  os.remove("/tmp/events.log")

spark.sql("CREATE DATABASE IF NOT EXISTS test_db")
spark.sql("DROP TABLE IF EXISTS test_db.t1")
spark.sql("DROP TABLE IF EXISTS test_db.t2")
spark.sql("CREATE TABLE test_db.t1 (id long, value string) LOCATION '/mnt/openlineage/test_db/t1'")

df = spark.sparkContext.parallelize( [(1,"a"),(2,"b"),(3,"c")]).toDF(["id", "value"])
df.write.mode("overwrite").saveAsTable("test_db.t1")

spark.sql("CREATE TABLE test_db.t2 AS select * from test_db.t1")

time.sleep(3)

dbutils.fs.cp(
  "file:/tmp/events.log",
  "dbfs:/databricks/openlineage/events.log"
)