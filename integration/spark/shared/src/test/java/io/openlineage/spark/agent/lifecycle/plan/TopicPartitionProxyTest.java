/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.util.stream.Stream;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class TopicPartitionProxyTest {
  private static final String TOPIC = "events";

  @ParameterizedTest
  @MethodSource("topicPartitions")
  void readsTopicAndPartition(Object topicPartition) {
    TopicPartitionProxy proxy = new TopicPartitionProxy(topicPartition);

    assertThat(proxy.topic()).contains(TOPIC);
    assertThat(proxy.partition()).contains(3);
  }

  static Stream<Object> topicPartitions() {
    return Stream.of(
        new TopicPartition(TOPIC, 3),
        new kafkashaded.org.apache.kafka.common.TopicPartition(TOPIC, 3));
  }

  @Test
  void rejectsNull() {
    assertThatIllegalArgumentException().isThrownBy(() -> new TopicPartitionProxy(null));
  }

  @Test
  void rejectsUnrelatedClassWithTheSameMethods() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new TopicPartitionProxy(new TopicPartitionLookalike()));
  }

  public static final class TopicPartitionLookalike {
    public String topic() {
      return TOPIC;
    }

    public int partition() {
      return 3;
    }
  }
}
