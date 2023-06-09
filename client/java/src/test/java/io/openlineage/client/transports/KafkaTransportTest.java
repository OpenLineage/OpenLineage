/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import static io.openlineage.client.Events.event;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineageClient;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class KafkaTransportTest {
  @Test
  void clientEmitsKafkaTransport() throws IOException {
    KafkaProducer<String, String> producer = mock(KafkaProducer.class);
    KafkaConfig config = new KafkaConfig();

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092;external:9092");

    config.setTopicName("test-topic");
    config.setProperties(properties);

    KafkaTransport transport = new KafkaTransport(producer, config);
    OpenLineageClient client = new OpenLineageClient(transport);

    when(producer.send(any(ProducerRecord.class))).thenReturn(mock(Future.class));

    client.emit(event());

    ArgumentCaptor<ProducerRecord<String, String>> captor =
        ArgumentCaptor.forClass(ProducerRecord.class);

    verify(producer, times(1)).send(captor.capture());

    assertThat(captor.getValue().topic()).isEqualTo("test-topic");
  }
}
