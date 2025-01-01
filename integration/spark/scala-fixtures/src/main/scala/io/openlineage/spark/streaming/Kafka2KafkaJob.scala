/**
 * Copyright 2018-2025 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.spark.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, from_unixtime, struct, to_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.slf4j.LoggerFactory

import java.time.Duration
import java.util

object Kafka2KafkaJob {
  private val log = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("io.openlineage.spark.streaming.Kafka2Kafka")
      .getOrCreate()
    runJob(spark)
  }

  private def runJob(spark: SparkSession): Unit = {
    try {
      val sourceStream = spark.readStream
        .format("kafka")
        .option("subscribe", "input-topic")
        .option("kafka.bootstrap.servers", "kafka.broker.zero:9092")
        .load()

      val fields = new util.ArrayList[StructField]()
      fields.add(StructField("registration_id", StringType, nullable = false))
      fields.add(StructField("registration_epoch", LongType, nullable = false))
      val eventSchema = StructType(fields)

      val processedEvents = sourceStream.select(col("value").cast(StringType).as("value"))
        .select(from_json(col("value"), eventSchema).as("registration_event"))
        .select(col("registration_event.registration_id").as("id"), col("registration_event.registration_epoch").as("epoch"))
        .select(col("id"), from_unixtime(col("epoch")).as("timestamp"))
        .select(struct(col("id"), col("timestamp")).as("value"))
        .select(to_json(col("value")).as("value"))

      val streamingQuery = processedEvents.writeStream
        .format("kafka")
        .option("topic", "output-topic")
        .option("kafka.bootstrap.servers", "kafka.broker.zero:9092")
        .option("checkpointLocation", "/tmp/checkpoint")
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
