/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.proxy.api.models;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * KafkaLineageStream is responsible for sending open lineage events to kafka. The collect() method
 * is called each time an open lineage event is emitted by the data platform.
 */
@Slf4j
public class KafkaLineageStream extends LineageStream {
  private final String topicName;
  private final String messageKey;
  private final KafkaProducer<String, String> producer;

  public KafkaLineageStream(@NonNull final KafkaConfig kafkaConfig) {
    super(Type.KAFKA);
    this.topicName = kafkaConfig.getTopicName();
    this.messageKey = kafkaConfig.getMessageKey();
    this.producer = new KafkaProducer<>(kafkaConfig.getProperties());
  }

  @Override
  public void collect(@NonNull String eventAsString) {
    log.debug("Received lineage event: {}", eventAsString);
    // if messageKey is not set, then the event will be sent to a random partition
    final ProducerRecord<String, String> record =
        new ProducerRecord<>(topicName, messageKey, eventAsString);
    try {
      producer.send(record);
    } catch (Exception e) {
      log.error("Failed to collect lineage event: {}", eventAsString, e);
    }
  }
}
