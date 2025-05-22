# Copyright 2018-2025 contributors to the OpenLineage project
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
    {"paths": ["/test_data/dynamic_data.json"]},
    "json",
    {"withHeader": False, "multiline": False},
)

# from os import system
#
# system("cat /test_data/dynamic_data.json")

# Create filtered DynamicFrame with custom lambda
# to filter records by Provider State and Provider City
filtered = data.filter(f=lambda x: x["name"] in ["Sally"] and x["location"]["state"] in ["WY"])

# filter.save()

# Compare record counts
print("Unfiltered record count: ", data.count())
print("Filtered record count:  ", filtered.count())
