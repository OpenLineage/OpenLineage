#!/bin/bash
#
# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
#
# You can run this on Windows as well, just change to a batch files.
#
# Note: You need the Databricks CLI installed, and you need a token configured.
#       You may need to add a "--profile" option to each command if you want to
#       upload to a Databricks workspace that is not configured as your default
#       profile for the Databricks CLI.
#


echo "Creating DBFS direcrtory"
dbfs mkdirs dbfs:/databricks/openlineage

echo "Uploading custom Spark Listener library"
dbfs cp --overwrite ./build/libs/openlineage-spark-*.jar               dbfs:/databricks/openlineage/

echo "Listing DBFS directory"
dbfs ls dbfs:/databricks/openlineage
