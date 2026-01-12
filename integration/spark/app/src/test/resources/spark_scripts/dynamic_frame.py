# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from pyspark.context import SparkContext
from awsglue.context import GlueContext

# Create GlueContext
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)

print("Config OL: ", sc.getConf().getAll())


# Create DynamicFrame from Glue Data Catalog
data = glueContext.create_dynamic_frame.from_options(
    "s3",
    {"paths": ["/test_data/dynamic_data.jsonl"]},
    "json",
    {"withHeader": False, "multiline": False},
)

# from os import system
#
# system("cat /test_data/dynamic_data.jsonl")

# Create filtered DynamicFrame with custom lambda
# to filter records by Provider State and Provider City
filtered = data.filter(f=lambda x: x["name"] in ["Sally"] and x["location"]["state"] in ["WY"])

filtered.count()  # this should create OL event with inputs from dynamic frame
filtered.toDF().write.mode("overwrite").json("/tmp/glue-test-job")  # this saves Dataframe

# Compare record counts
print("Unfiltered record count: ", data.count())
print("Filtered record count:  ", filtered.count())
