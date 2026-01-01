# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os

from pyspark.sql import SparkSession

os.makedirs("/tmp/cll_test", exist_ok=True)


spark = (
    SparkSession.builder.master("local")
    .appName("Open Lineage Integration Create Table")
    .config("spark.sql.warehouse.dir", "file:/tmp/cll_test/")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("info")

spark.sql("CREATE TABLE cll_source1 (a int, b string)")
spark.sql("CREATE TABLE cll_source2 (a int, c int)")

spark.sql(
    """
CREATE TABLE tbl1 
USING hive 
LOCATION '/tmp/cll_test/tbl1' 
AS SELECT 
    t1.a as ident,
    CONCAT(b, 'test') as trans,
    SUM(c) as agg
FROM 
    (SELECT a, c from cll_source2 where a > 1) t2
JOIN cll_source1 t1 on t1.a = t2.a 
GROUP BY t1.a, b
"""
)
