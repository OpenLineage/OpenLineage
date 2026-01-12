/** 
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.spark.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.slf4j.LoggerFactory

import java.time.Duration

object KinesisReadJob {
  private val log = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("io.openlineage.spark.streaming.KinesisReadJob")
      .getOrCreate()
    runJob(spark)
  }

  private def runJob(spark: SparkSession): Unit = {
    try {
      val sourceStream = spark.readStream
        .format("aws-kinesis")
        .option("kinesis.region", "localstack")
        .option("kinesis.kinesisRegion", "localstack")
        .option("kinesis.streamName", "events")
        .option("kinesis.consumerType", "GetRecords")
        .option("kinesis.endpointUrl", "http://localstack:4566")
        .option("kinesis.startingposition", "LATEST")
        .load()

      val streamingQuery = sourceStream.writeStream
        .format("console")
        .trigger(Trigger.ProcessingTime(Duration.ofSeconds(1).toMillis))
        .start()

      streamingQuery.awaitTermination(Duration.ofSeconds(30).toMillis)
    } catch {
      case e: Exception => log.error("Caught an exception", e)
    }finally {
      spark.stop()
    }
  }
}
