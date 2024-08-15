/**
 * Copyright 2018-2024 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.spark.iceberg

import org.apache.spark.sql.SparkSession

object ReadIcebergMetadataJsonOutsideConfiguredCatalogJob {
  def main(args: Array[String]): Unit = {
    val metadataJsonFilePath = args(0)
    val spark = SparkSession.builder().getOrCreate()

    spark.sql("CREATE DATABASE IF NOT EXISTS openlineage_workspace")
    spark.read.format("iceberg")
      .load(metadataJsonFilePath)
      .write
      .format("iceberg")
      .saveAsTable("openlineage_workspace.person")

    spark.stop()
  }
}
