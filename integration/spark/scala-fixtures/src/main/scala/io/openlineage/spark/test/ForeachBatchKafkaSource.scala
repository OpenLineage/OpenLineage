/**
 * Copyright 2018-2024 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.spark.test

import org.apache.spark.sql.functions.{col, from_json, lit}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{Metadata, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import java.time.Duration
import java.util

object ForeachBatchKafkaSource {
  private val SPARK_TEST_SOURCE_TOPIC_PROP = "spark.test.source.topic"
  private val SPARK_TEST_CHECKPOINT_LOCATION_PROP = "spark.test.checkpoint.location"
  private val SPARK_TEST_OUTPUT_LOCATION_PROP = "spark.test.output.location"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("foreach-batch-kafka-source")
      .getOrCreate()

    val sourceTopic = getConfigOrThrow(spark, SPARK_TEST_SOURCE_TOPIC_PROP)
    val checkpointLocation = getConfigOrThrow(spark, SPARK_TEST_CHECKPOINT_LOCATION_PROP)

    val sourceStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", sourceTopic)
      .option("startingOffsets", "earliest")
      .load()

    val streamingQuery = sourceStream.writeStream
      .option("checkpointLocation", checkpointLocation)
      .foreachBatch((dataset: Dataset[Row], batchId: Long) => writeBatch(dataset, batchId))
      .trigger(Trigger.ProcessingTime(Duration.ofSeconds(5).toMillis))
      .start()

    streamingQuery.awaitTermination(Duration.ofSeconds(30).toMillis)
    spark.stop()
  }

  private def writeBatch(batch: Dataset[Row], batchId: Long): Unit = {
    val spark = batch.sparkSession
    val outputLocation = getConfigOrThrow(spark, SPARK_TEST_OUTPUT_LOCATION_PROP)

    val fields = new util.ArrayList[StructField]()
    fields.add(StructField("registration_id", StringType, nullable = true, metadata = Metadata.empty))
    fields.add(StructField("registration_datetime", StringType, nullable = true, metadata = Metadata.empty))
    val schema = StructType(fields)

    batch.select(
        col("value").cast(StringType).as("value"),
        lit(batchId).as("batch_id"))
      .select(
        from_json(col("value"), schema).as("payload"),
        col("batch_id"))
      .select(
        col("payload.registration_id").as("registration_id"),
        col("payload.registration_datetime").as("registration_datetime"),
        col("batch_id"))
      .write
      .partitionBy("batch_id")
      .format("parquet")
      .mode("overwrite")
      .save(outputLocation)
  }

  private def getConfigOrThrow(spark: SparkSession, property: String): String = {
    val conf = spark.conf
    val opt = Option(conf.get(property, null))
      .orElse(Option(System.getProperty(property)))
      .orElse(Option(System.getenv(asEnvVar(property))))

    if (opt.isEmpty) {
      spark.stop()
      throw new IllegalArgumentException(s"Missing parameter: ${asParameter(property)}. Do ONE of the following to rectify this: " +
        s"(1) Specify the spark property '$property', " +
        s"(2) Specify the system property '$property', or " +
        s"(3) Specify the environment variable '${asEnvVar(property)}'")
    }

    opt.get
  }

  private def asParameter(str: String): String = str.replace("spark.test.", "")

  private def asEnvVar(str: String): String = str.toUpperCase.replace(".", "_")
}
