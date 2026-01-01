/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink;

import io.openlineage.flink.avro.event.OutputEvent;
import java.util.Arrays;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.formats.avro.AvroSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicsDescriptor;
import org.apache.kafka.clients.producer.ProducerRecord;

public class MultiTopicSerializationSchema extends KafkaTopicsDescriptor implements
    KafkaRecordSerializationSchema<OutputEvent> {

  public MultiTopicSerializationSchema(String... topics) {
    super(Arrays.asList(topics), null);
  }

  @Override
  public void open(
      SerializationSchema.InitializationContext context,
      KafkaSinkContext sinkContext)
      throws Exception {
    KafkaRecordSerializationSchema.super.open(context, sinkContext);
  }

  @Override
  public ProducerRecord<byte[], byte[]> serialize(
      OutputEvent t, KafkaSinkContext kafkaSinkContext, Long aLong) {
    AvroSerializationSchema<OutputEvent> serializer = AvroSerializationSchema.forSpecific(OutputEvent.class);
    return new ProducerRecord<>(getTopic(t), serializer.serialize(t));
  }

  public String getTopic(OutputEvent outputEvent) {
    if (outputEvent.counter % 2 == 0) {
      return getFixedTopics().get(0);
    } else {
      return getFixedTopics().get(1);
    }
  }
}
