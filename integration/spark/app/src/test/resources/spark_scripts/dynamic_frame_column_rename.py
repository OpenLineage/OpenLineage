# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""
Test script for Glue DynamicFrame with column renames.
Reproduces the bug where 'orderid' -> 'order_id' lineage is captured as 'order_id' -> 'order_id'
"""

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F

# Create GlueContext
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

print("Config OL: ", sc.getConf().getAll())

# Create test data with 'orderid' column (no underscore)
data = [
    {"orderid": "ORD-001", "amount": 100.0},
    {"orderid": "ORD-002", "amount": 200.0},
]

# Create DataFrame with lowercase 'orderid'
df = spark.createDataFrame(data)

# Write source data
df.write.mode("overwrite").parquet("/test_data/source_orders")

# Create DynamicFrame from the source
dyf_source = glueContext.create_dynamic_frame.from_options(
    "parquet",
    {"paths": ["/test_data/source_orders"]},
)

# Convert to DataFrame
df_orders = dyf_source.toDF()

# Apply transformation: rename 'orderid' to 'order_id' (with underscore)
df_transformed = df_orders.select(
    F.col("orderid").alias("order_id"),  # This is the rename
    F.col("amount"),
)

# Write result
df_transformed.write.mode("overwrite").parquet("/tmp/glue-column-rename-test")

print("Test completed. Check column lineage for 'orderid' -> 'order_id' mapping")
