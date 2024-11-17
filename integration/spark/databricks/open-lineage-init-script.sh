#!/bin/bash
#
# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

STAGE_DIR="/dbfs/databricks/openlineage"

echo "BEGIN: Upload Spark Listener JARs"
cp -f $STAGE_DIR/openlineage-spark_*.jar /mnt/driver-daemon/jars || { echo "Error copying Spark Listener library file"; exit 1;}
echo "END: Upload Spark Listener JARs"

echo "BEGIN: Modify Spark config settings"
cat << 'EOF' > /databricks/driver/conf/openlineage-spark-driver-defaults.conf
[driver] {
  "spark.extraListeners" = "com.databricks.backend.daemon.driver.DBCEventLoggingListener,io.openlineage.spark.agent.OpenLineageSparkListener"
}
EOF
echo "END: Modify Spark config settings"
