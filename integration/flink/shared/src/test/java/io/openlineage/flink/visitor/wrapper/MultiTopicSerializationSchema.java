/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.wrapper;

import java.util.Arrays;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicsDescriptor;
import org.apache.kafka.clients.producer.ProducerRecord;

public class MultiTopicSerializationSchema extends KafkaTopicsDescriptor
    implements KafkaRecordSerializationSchema<SomeEvent> {

  public MultiTopicSerializationSchema(String... topics) {
    super(Arrays.asList(topics), null);
  }

  @Override
  public void open(SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext)
      throws Exception {
    KafkaRecordSerializationSchema.super.open(context, sinkContext);
  }

  @Override
  public ProducerRecord<byte[], byte[]> serialize(
      SomeEvent t, KafkaSinkContext kafkaSinkContext, Long aLong) {
    throw new RuntimeException("unimplemented");
  }
}
