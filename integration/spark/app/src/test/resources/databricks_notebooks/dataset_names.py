# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os
import time

runtime_version = os.environ.get("DATABRICKS_RUNTIME_VERSION", None).replace(".", "_")

dbutils.fs.rm("dbfs:/mnt/openlineage/test_db/t1_{}'".format(runtime_version), True)
dbutils.fs.mkdirs("dbfs:/mnt/openlineage/test_db/t1_{}'".format(runtime_version))

if os.path.exists("/tmp/events.log"):
    os.remove("/tmp/events.log")

spark.sql("CREATE DATABASE IF NOT EXISTS test_db")
spark.sql("DROP TABLE IF EXISTS test_db.t1_{}".format(runtime_version))
spark.sql("DROP TABLE IF EXISTS test_db.t2_{}".format(runtime_version))
spark.sql(
    "CREATE TABLE test_db.t1_{} (id long, value string) LOCATION '/mnt/openlineage/test_db/t1_{}'".format(
        runtime_version, runtime_version
    )
)

df = spark.sparkContext.parallelize([(1, "a"), (2, "b"), (3, "c")]).toDF(["id", "value"])
df.write.mode("overwrite").saveAsTable("test_db.t1_{}".format(runtime_version))

spark.sql(
    "CREATE TABLE test_db.t2_{} AS select * from test_db.t1_{}".format(runtime_version, runtime_version)
)

time.sleep(3)

event_file = "dbfs:/databricks/openlineage/events_{}.log".format(runtime_version)
dbutils.fs.rm(event_file, True)
dbutils.fs.cp("file:/tmp/events.log", event_file)
