# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os
import time

runtime_version = os.environ.get("DATABRICKS_RUNTIME_VERSION", None).replace(".", "_")
source_table = "default.rtas_source_{}".format(runtime_version)
target_table = "default.rtas_target_{}".format(runtime_version)

spark.sql("DROP TABLE IF EXISTS {}".format(source_table))
spark.sql("DROP TABLE IF EXISTS {}".format(target_table))

spark.createDataFrame([{"a": 1, "b": 2}, {"a": 3, "b": 4}]).write.format(
    "delta"
).saveAsTable(source_table)
spark.sql("CREATE TABLE {} USING delta AS SELECT * FROM {}".format(target_table, source_table))

# Isolate the historical overwrite operation from setup events.
time.sleep(3)
with open("/tmp/events.log", "w", encoding="utf-8"):
    pass

left = spark.table(source_table).alias("left")
right = spark.table(source_table).alias("right")
final_df = left.join(right, left.a == right.a).select(left.a, right.b)
final_df.write.format("delta").mode("overwrite").saveAsTable(target_table)

time.sleep(3)
event_file = "dbfs:/databricks/openlineage/events_{}.log".format(runtime_version)
dbutils.fs.rm(event_file, True)
dbutils.fs.cp("file:/tmp/events.log", event_file)
