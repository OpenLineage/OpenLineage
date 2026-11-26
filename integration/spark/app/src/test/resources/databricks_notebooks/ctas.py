# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os
import sys
import time

if os.path.exists("/tmp/events.log"):
    os.remove("/tmp/events.log")

# suffix for names and paths, e.g. 16_4_scala2_13, passed by DatabricksEnvironment.runScript
runtime_version = sys.argv[1]

spark.sql("DROP TABLE IF EXISTS default.temp_{}".format(runtime_version))
spark.sql("DROP TABLE IF EXISTS default.ctas_{}".format(runtime_version))

spark.createDataFrame([{"a": 1, "b": 2}, {"a": 3, "b": 4}]).repartition(1).write.mode(
    "overwrite"
).saveAsTable("default.temp_{}".format(runtime_version))

spark.sql(
    "CREATE TABLE default.ctas_{} AS SELECT a, b FROM default.temp_{}".format(
        runtime_version, runtime_version
    )
)

time.sleep(3)

event_file = "dbfs:/databricks/openlineage/events_{}.log".format(runtime_version)
dbutils.fs.rm(event_file, True)
dbutils.fs.cp("file:/tmp/events.log", event_file)
