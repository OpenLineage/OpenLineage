# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os
import time

runtime_version = os.environ.get("DATABRICKS_RUNTIME_VERSION", None).replace(".", "_")

dbutils.fs.rm("dbfs:/user/hive/warehouse/test_db.db/updates_{}".format(runtime_version), True)
dbutils.fs.rm("dbfs:/user/hive/warehouse/test_db.db/events_{}".format(runtime_version), True)

dbutils.fs.mkdirs("dbfs:/user/hive/warehouse/test_db.db/updates_{}".format(runtime_version))
dbutils.fs.mkdirs("dbfs:/user/hive/warehouse/test_db.db/events_{}".format(runtime_version))

if os.path.exists("/tmp/events.log"):
    os.remove("/tmp/events.log")

spark.sql("CREATE DATABASE IF NOT EXISTS test_db")
spark.sql("DROP TABLE IF EXISTS test_db.events_{}".format(runtime_version))
spark.sql("DROP TABLE IF EXISTS test_db.updates_{}".format(runtime_version))

spark.sql(
    "CREATE TABLE test_db.events_{} (event_id long, last_updated_at long) USING delta".format(runtime_version)
)
spark.sql(
    "CREATE TABLE test_db.updates_{} (event_id long, updated_at long) USING delta".format(runtime_version)
)

spark.sql("INSERT INTO test_db.events_{} VALUES (1, 1641290276);".format(runtime_version))
spark.sql("INSERT INTO test_db.updates_{} VALUES (1, 1641290277);".format(runtime_version))
spark.sql("INSERT INTO test_db.updates_{} VALUES (2, 1641290277);".format(runtime_version))

spark.sql(
    (
        "MERGE INTO test_db.events_{} target USING test_db.updates_{} "
        + " ON target.event_id = test_db.updates_{}.event_id"
        + " WHEN MATCHED THEN UPDATE SET target.last_updated_at = test_db.updates_{}.updated_at"
        + " WHEN NOT MATCHED THEN INSERT (event_id, last_updated_at) "
        + "VALUES (event_id, updated_at)"
    ).format(runtime_version, runtime_version, runtime_version, runtime_version)
)

time.sleep(3)

event_file = "dbfs:/databricks/openlineage/events_{}.log".format(runtime_version)
dbutils.fs.rm(event_file, True)
dbutils.fs.cp("file:/tmp/events.log", event_file)
