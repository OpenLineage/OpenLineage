# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os
import time
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Adjust the catalog name as necessary
catalog = "PROVIDE_NAME"

if os.path.exists("/tmp/events.log"):
    os.remove("/tmp/events.log")

runtime_version = os.environ.get("DATABRICKS_RUNTIME_VERSION", None).replace(".", "_")

schema = f"{catalog}.default"
tbl1 = f"{schema}.tbl1_{runtime_version}"
tbl2 = f"{schema}.tbl2_{runtime_version}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
spark.sql(f"DROP TABLE IF EXISTS {tbl1}")
spark.sql(f"DROP TABLE IF EXISTS {tbl2}")

tbl1_df = spark.createDataFrame(
    [
        (1, "alpha", 10),
        (2, "beta", 20),
        (3, "gamma", 30),
        (4, "delta", 40),
    ],
    schema=StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("value", IntegerType(), True),
        ]
    ),
)
tbl1_df.write.format("delta").saveAsTable(tbl1)

tbl2_df = spark.table(tbl1).withColumn("value_x2", f.col("value") * f.lit(2))
tbl2_df.write.format("delta").saveAsTable(tbl2)

time.sleep(3)

event_file = "dbfs:/databricks/openlineage/events_{}.log".format(runtime_version)
dbutils.fs.rm(event_file, True)
dbutils.fs.cp("file:/tmp/events.log", event_file)
