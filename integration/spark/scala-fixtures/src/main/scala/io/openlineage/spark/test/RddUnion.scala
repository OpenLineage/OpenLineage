/**
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.spark.test

import org.apache.spark.sql.{SaveMode, SparkSession}

object RddUnion extends App {
  val spark = SparkSession.builder().appName("RddUnion").getOrCreate()
  val sc = spark.sparkContext
  import spark.implicits._

  val dataDir = System.getProperty("data.dir", "/tmp/scala-test")

  sc
    .parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).toIndexedSeq)
    .map(a => a.toString)
    .toDF()
    .write
    .mode(SaveMode.Overwrite)
    .parquet(s"$dataDir/rdd_input1")

  sc
    .parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).toIndexedSeq)
    .map(a => a.toString)
    .toDF()
    .write
    .mode(SaveMode.Overwrite)
    .parquet(s"$dataDir/rdd_input2")

  val rdd1 = spark.read.parquet(s"$dataDir/rdd_input1").rdd
  val rdd2 = spark.read.parquet(s"$dataDir/rdd_input2").rdd

  rdd1
    .union(rdd2)
    .map(i => Entry(i.toString))
    .toDF()
    .write
    .mode(SaveMode.Overwrite)
    .parquet(s"$dataDir/rdd_output")
}
