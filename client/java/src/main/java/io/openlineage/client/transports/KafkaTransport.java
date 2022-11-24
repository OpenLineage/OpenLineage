/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.Properties;

@Slf4j
public final class KafkaTransport extends Transport {
  private final String topicName;
  private final String localServerId;
  private final KafkaProducer<String, String> producer;

  public KafkaTransport(@NonNull final KafkaConfig kafkaConfig) {
    this(new KafkaProducer<>(getProperties(kafkaConfig)), kafkaConfig);
  }

  private static Properties getProperties(@NotNull KafkaConfig kafkaConfig) {
    Properties properties = new Properties();
    properties.putAll(kafkaConfig.getProperties());
    return properties;
  }

  public KafkaTransport(
      @NonNull final KafkaProducer<String, String> kafkaProducer,
      @NonNull final KafkaConfig kafkaConfig) {
    super(Type.KAFKA);
    this.topicName = kafkaConfig.getTopicName();
    this.localServerId = kafkaConfig.getLocalServerId();
    this.producer = kafkaProducer;
  }

  @Override
  public void emit(@NonNull OpenLineage.RunEvent runEvent) {
    final String eventAsJson = OpenLineageClientUtils.toJson(runEvent);
    log.debug("Received lineage event: {}", eventAsJson);
    final ProducerRecord<String, String> record =
        new ProducerRecord<>(topicName, localServerId, eventAsJson);
    try {
      producer.send(record);
    } catch (Exception e) {
      log.error("Failed to collect lineage event: {}", eventAsJson, e);
    }
  }
}
