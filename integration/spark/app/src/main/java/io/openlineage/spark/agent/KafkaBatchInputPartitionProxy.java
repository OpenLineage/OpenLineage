/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.spark.sql.connector.read.InputPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class KafkaBatchInputPartitionProxy {
  public static final String KAFKA_BATCH_INPUT_PARTITION_CLASS_NAME =
      "org.apache.spark.sql.kafka010.KafkaBatchInputPartition";

  private static final Logger log = LoggerFactory.getLogger(KafkaBatchInputPartitionProxy.class);

  private final InputPartition wrappedPartition;

  public KafkaBatchInputPartitionProxy(InputPartition inputPartition) {
    if (!inputPartition
        .getClass()
        .getCanonicalName()
        .equals(KAFKA_BATCH_INPUT_PARTITION_CLASS_NAME)) {
      throw new IllegalArgumentException(
          "The type of inputPartition MUST be " + KAFKA_BATCH_INPUT_PARTITION_CLASS_NAME);
    }
    this.wrappedPartition = inputPartition;
  }

  public String topic() {
    try {
      Object offsetRange = MethodUtils.invokeMethod(wrappedPartition, "offsetRange");
      return (String) MethodUtils.invokeMethod(offsetRange, "topic");
    } catch (NoSuchMethodException e) {
      log.warn(
          "NoSuchMethodException occurred in topic() method. The method offsetRange() or topic() may not exist in the current context",
          e);
      return null;
    } catch (IllegalAccessException e) {
      log.warn(
          "IllegalAccessException occurred in topic() method. There was an attempt to access a method (offsetRange() or topic()) which is not accessible from the current context",
          e);
      return null;
    } catch (InvocationTargetException e) {
      log.warn(
          "InvocationTargetException occurred in topic() method. The called method (offsetRange() or topic()) threw an exception",
          e);
      return null;
    }
  }

  public Integer partition() {
    try {
      Object offsetRange = MethodUtils.invokeMethod(wrappedPartition, "offsetRange");
      return Integer.valueOf(MethodUtils.invokeMethod(offsetRange, "partition").toString());
    } catch (NoSuchMethodException e) {
      log.warn(
          "NoSuchMethodException occurred in partition() method. The method offsetRange() or partition() may not exist in the current context",
          e);
      return null;
    } catch (IllegalAccessException e) {
      log.warn(
          "IllegalAccessException occurred in partition() method. There was an attempt to access a method (offsetRange() or partition()) which is not accessible from the current context",
          e);
      return null;
    } catch (InvocationTargetException e) {
      log.warn(
          "InvocationTargetException occurred in partition() method. The called method (offsetRange() or partition()) threw an exception",
          e);
      return null;
    }
  }

  public String bootstrapServers() {
    try {
      Map<String, Object> executorKafkaParams =
          (Map<String, Object>) MethodUtils.invokeMethod(wrappedPartition, "executorKafkaParams");
      return (String) executorKafkaParams.get("bootstrap.servers");
    } catch (NoSuchMethodException e) {
      log.warn(
          "NoSuchMethodException occurred in bootstrapServers() method. The method executorKafkaParams() may not exist in the current context",
          e);
      return null;
    } catch (IllegalAccessException e) {
      log.warn(
          "IllegalAccessException occurred in bootstrapServers() method. There was an attempt to access a method (executorKafkaParams()) which is not accessible from the current context",
          e);
      return null;
    } catch (InvocationTargetException e) {
      log.warn(
          "InvocationTargetException occurred in bootstrapServers() method. The called method (executorKafkaParams()) threw an exception",
          e);
      return null;
    }
  }
}
