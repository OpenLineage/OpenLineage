/** 
 * Copyright 2018-2025 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.spark.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.slf4j.LoggerFactory

import java.time.Duration

object MongoStreamingJob {
  private val log = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MongoStreamingJob")
      .getOrCreate()
    runJob(spark)
  }

  private def runJob(spark: SparkSession): Unit = {
    try {
      val readSchema = new StructType()
        .add("_id", DataTypes.StringType)
        .add("name", DataTypes.StringType)
        .add("date", DataTypes.TimestampType)
        .add("location", DataTypes.StringType)

      val sourceStream = spark.readStream
        .format("mongodb")
        .option("spark.mongodb.change.stream.publish.full.document.only", "true")
        .option("spark.mongodb.connection.uri", "mongodb://m1:27017")
        .option("spark.mongodb.database", "events")
        .option("spark.mongodb.collection", "events")
        .option("forceDeleteTempCheckpointLocation", "true")
        .option("spark.mongodb.change.stream.change.stream.full.document", "updateLookup")
        .schema(readSchema)
        .load()

      sourceStream
        .writeStream
        .format("console")
        .option("truncate", "false")
        .trigger(Trigger.ProcessingTime(Duration.ofSeconds(4).toMillis))
        .start()
        .awaitTermination(Duration.ofSeconds(30).toMillis)
    } catch {
      case e: Exception => log.error("Caught an exception", e)
    } finally {
      spark.stop()
    }

  }
}
