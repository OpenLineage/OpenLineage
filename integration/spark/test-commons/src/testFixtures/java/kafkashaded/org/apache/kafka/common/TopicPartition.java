/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package kafkashaded.org.apache.kafka.common;

/** Test fixture for the Kafka class name used by Databricks. */
public final class TopicPartition {
  private final String topic;
  private final int partition;

  public TopicPartition(String topic, int partition) {
    this.topic = topic;
    this.partition = partition;
  }

  public String topic() {
    return topic;
  }

  public int partition() {
    return partition;
  }
}
