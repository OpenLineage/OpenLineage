/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import java.lang.reflect.InvocationTargetException;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.MethodUtils;

/**
 * A proxy class for TopicPartition in Kafka. This class is used to interact with the TopicPartition
 * object without directly depending on the Kafka library.
 */
@Slf4j
final class TopicPartitionProxy {
  private static final String EXPECTED_CLASS_NAME = "org.apache.kafka.common.TopicPartition";
  private final Object topicPartition;

  /**
   * Constructs a new TopicPartitionProxy.
   *
   * @param topicPartition the TopicPartition object from Kafka library
   * @throws IllegalArgumentException if the provided object is null or not an instance of
   *     TopicPartition
   */
  public TopicPartitionProxy(Object topicPartition) {
    if (topicPartition == null) {
      throw new IllegalArgumentException("constructor argument cannot be null");
    }

    if (!EXPECTED_CLASS_NAME.equals(topicPartition.getClass().getCanonicalName())) {
      throw new IllegalArgumentException(
          "constructor argument must be of type" + EXPECTED_CLASS_NAME);
    }

    this.topicPartition = topicPartition;
  }

  /**
   * Retrieves the topic name from the TopicPartition object.
   *
   * @return an Optional containing the topic name, or an empty Optional if the method invocation
   *     fails
   */
  public Optional<String> topic() {
    return Optional.ofNullable(tryInvokeMethod("topic"));
  }

  /**
   * Retrieves the partition number from the TopicPartition object.
   *
   * @return an Optional containing the partition number, or an empty Optional if the method
   *     invocation fails
   */
  public Optional<Integer> partition() {
    return Optional.ofNullable(tryInvokeMethod("partition"));
  }

  /**
   * Tries to invoke a method on the TopicPartition object.
   *
   * @param methodName the name of the method to invoke
   * @return the result of the method invocation, or null if the invocation fails
   */
  @Nullable
  private <T> T tryInvokeMethod(String methodName) {
    T result = null;
    try {
      result = (T) MethodUtils.invokeMethod(topicPartition, methodName);
    } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
      log.error(
          "Method '{}' could not be invoked on object with type '{}'",
          methodName,
          topicPartition.getClass().getCanonicalName(),
          e);
    }
    return result;
  }
}
