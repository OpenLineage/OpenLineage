/**
 * Copyright 2018-2025 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.spark.iceberg

import org.apache.spark.sql.SparkSession

import java.nio.file.Paths

object ReadIcebergMetadataJsonWithoutAConfiguredIcebergCatalog {
  def main(args: Array[String]): Unit = {
    val metadataJsonFilePath = args(0)
    val spark = SparkSession.builder().getOrCreate()

    val sparkWarehousePath = spark.conf.get("spark.sql.warehouse.dir")
    spark.sql("CREATE DATABASE IF NOT EXISTS openlineage_workspace")
    spark.read.format("iceberg")
      .load(metadataJsonFilePath)
      .write
      .format("parquet")
      .save(Paths.get(sparkWarehousePath).resolve("person").toString)

    spark.stop()
  }
}
