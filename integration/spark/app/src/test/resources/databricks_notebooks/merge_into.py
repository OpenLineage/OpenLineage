# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os
import time

dbutils.fs.rm("dbfs:/mnt/openlineage/test/updates", True)
dbutils.fs.rm("dbfs:/mnt/openlineage/test/events", True)

dbutils.fs.mkdirs("dbfs:/mnt/openlineage/test/updates")
dbutils.fs.mkdirs("dbfs:/mnt/openlineage/test/events")

if os.path.exists("/tmp/events.log"):
    os.remove("/tmp/events.log")

spark.sql("CREATE DATABASE IF NOT EXISTS test_db")
spark.sql("DROP TABLE IF EXISTS test_db.events")
spark.sql("DROP TABLE IF EXISTS test_db.updates")

spark.sql("CREATE TABLE test_db.events (event_id long, last_updated_at long) USING delta")
spark.sql("CREATE TABLE test_db.updates (event_id long, updated_at long) USING delta")

spark.sql("INSERT INTO test_db.events VALUES (1, 1641290276);")
spark.sql("INSERT INTO test_db.updates VALUES (1, 1641290277);")
spark.sql("INSERT INTO test_db.updates VALUES (2, 1641290277);")

spark.sql(
    "MERGE INTO test_db.events target USING test_db.updates "
    + " ON target.event_id = test_db.updates.event_id"
    + " WHEN MATCHED THEN UPDATE SET target.last_updated_at = test_db.updates.updated_at"
    + " WHEN NOT MATCHED THEN INSERT (event_id, last_updated_at) "
    + "VALUES (event_id, updated_at)"
)

time.sleep(3)

dbutils.fs.cp("file:/tmp/events.log", "dbfs:/databricks/openlineage/events.log")
