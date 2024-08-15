/**
 * Copyright 2018-2024 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.spark.iceberg

import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Row, SparkSession}

import java.util

object CreateSeedDataJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    spark.sql("CREATE DATABASE openlineage_public")
    spark.sql("CREATE TABLE openlineage_public.person ( first_name STRING, last_name STRING, age INT ) USING Iceberg")
    val row = Row("Petr", "Pavel", 62)
    val rows = new util.ArrayList[Row]()
    rows.add(row)

    val fields = Array(
      DataTypes.createStructField("first_name", DataTypes.StringType, false),
      DataTypes.createStructField("last_name", DataTypes.StringType, false),
      DataTypes.createStructField("age", DataTypes.IntegerType, false)
    )
    val schema = DataTypes.createStructType(fields)

    val dataframe = spark.createDataFrame(rows, schema)
    dataframe.createOrReplaceTempView("_person")
    spark.sql("INSERT INTO openlineage_public.person SELECT first_name, last_name, age FROM _person")

    spark.stop()
  }
}
