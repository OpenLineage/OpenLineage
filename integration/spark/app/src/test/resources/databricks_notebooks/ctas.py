# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os, time;

if os.path.exists("/tmp/events.log"):
  os.remove("/tmp/events.log")

spark.sql("DROP TABLE IF EXISTS default.temp")
spark.sql("DROP TABLE IF EXISTS default.ctas")

spark.createDataFrame([
  {'a': 1, 'b': 2},
  {'a': 3, 'b': 4}
]).repartition(1).write.mode("overwrite").saveAsTable("default.temp")

spark.sql("CREATE TABLE default.ctas AS SELECT a, b FROM default.temp")

time.sleep(3)

dbutils.fs.cp(
  "file:/tmp/events.log",
  "dbfs:/databricks/openlineage/events.log"
)