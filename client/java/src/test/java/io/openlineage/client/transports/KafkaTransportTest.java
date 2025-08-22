/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import static io.openlineage.client.Events.datasetEvent;
import static io.openlineage.client.Events.emptyRunEvent;
import static io.openlineage.client.Events.jobEvent;
import static io.openlineage.client.Events.runEvent;
import static io.openlineage.client.Events.runEventWithParent;
import static io.openlineage.client.Events.runEventWithRootParent;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.OpenLineageClientUtils;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class KafkaTransportTest {
  @Test
  void clientEmitsRunEventKafkaTransport() throws IOException {
    KafkaProducer<String, String> producer = mock(KafkaProducer.class);
    KafkaConfig config = new KafkaConfig();

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092;external:9092");

    config.setTopicName("test-topic");
    config.setProperties(properties);

    KafkaTransport transport = new KafkaTransport(producer, config);
    OpenLineageClient client = new OpenLineageClient(transport);

    when(producer.send(any(ProducerRecord.class))).thenReturn(mock(Future.class));

    OpenLineage.RunEvent event = runEvent();
    client.emit(event);

    ArgumentCaptor<ProducerRecord<String, String>> captor =
        ArgumentCaptor.forClass(ProducerRecord.class);

    verify(producer, times(1)).send(captor.capture());

    assertThat(captor.getValue().topic()).isEqualTo("test-topic");
    assertThat(captor.getValue().key()).isEqualTo("run:test-namespace/test-job");
    assertThat(captor.getValue().value()).isEqualTo(OpenLineageClientUtils.toJson(event));
  }

  @Test
  void clientEmitsRunEventWithRootParentKafkaTransport() throws IOException {
    KafkaProducer<String, String> producer = mock(KafkaProducer.class);
    KafkaConfig config = new KafkaConfig();

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092;external:9092");

    config.setTopicName("test-topic");
    config.setProperties(properties);

    KafkaTransport transport = new KafkaTransport(producer, config);
    OpenLineageClient client = new OpenLineageClient(transport);

    when(producer.send(any(ProducerRecord.class))).thenReturn(mock(Future.class));

    OpenLineage.RunEvent event = runEventWithRootParent();
    client.emit(event);

    ArgumentCaptor<ProducerRecord<String, String>> captor =
        ArgumentCaptor.forClass(ProducerRecord.class);

    verify(producer, times(1)).send(captor.capture());

    assertThat(captor.getValue().topic()).isEqualTo("test-topic");
    assertThat(captor.getValue().key()).isEqualTo("run:root-namespace/root-job");
    assertThat(captor.getValue().value()).isEqualTo(OpenLineageClientUtils.toJson(event));
  }

  @Test
  void clientEmitsRunEventWithParentKafkaTransport() throws IOException {
    KafkaProducer<String, String> producer = mock(KafkaProducer.class);
    KafkaConfig config = new KafkaConfig();

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092;external:9092");

    config.setTopicName("test-topic");
    config.setProperties(properties);

    KafkaTransport transport = new KafkaTransport(producer, config);
    OpenLineageClient client = new OpenLineageClient(transport);

    when(producer.send(any(ProducerRecord.class))).thenReturn(mock(Future.class));

    OpenLineage.RunEvent event = runEventWithParent();
    client.emit(event);

    ArgumentCaptor<ProducerRecord<String, String>> captor =
        ArgumentCaptor.forClass(ProducerRecord.class);

    verify(producer, times(1)).send(captor.capture());

    assertThat(captor.getValue().topic()).isEqualTo("test-topic");
    assertThat(captor.getValue().key()).isEqualTo("run:parent-namespace/parent-job");
    assertThat(captor.getValue().value()).isEqualTo(OpenLineageClientUtils.toJson(event));
  }

  @Test
  void clientEmitsRunEventKafkaTransportWithExplicitMessageKey() throws IOException {
    KafkaProducer<String, String> producer = mock(KafkaProducer.class);
    KafkaConfig config = new KafkaConfig();

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092;external:9092");

    config.setTopicName("test-topic");
    config.setMessageKey("explicit-key");
    config.setProperties(properties);

    KafkaTransport transport = new KafkaTransport(producer, config);
    OpenLineageClient client = new OpenLineageClient(transport);

    when(producer.send(any(ProducerRecord.class))).thenReturn(mock(Future.class));

    OpenLineage.RunEvent event = runEvent();
    client.emit(event);

    ArgumentCaptor<ProducerRecord<String, String>> captor =
        ArgumentCaptor.forClass(ProducerRecord.class);

    verify(producer, times(1)).send(captor.capture());

    assertThat(captor.getValue().topic()).isEqualTo("test-topic");
    assertThat(captor.getValue().key()).isEqualTo("explicit-key");
    assertThat(captor.getValue().value()).isEqualTo(OpenLineageClientUtils.toJson(event));
  }

  @Test
  void clientEmitsEmptyRunEventKafkaTransport() throws IOException {
    KafkaProducer<String, String> producer = mock(KafkaProducer.class);
    KafkaConfig config = new KafkaConfig();

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092;external:9092");

    config.setTopicName("test-topic");
    config.setProperties(properties);

    KafkaTransport transport = new KafkaTransport(producer, config);
    OpenLineageClient client = new OpenLineageClient(transport);

    when(producer.send(any(ProducerRecord.class))).thenReturn(mock(Future.class));

    OpenLineage.RunEvent event = emptyRunEvent();
    client.emit(event);

    ArgumentCaptor<ProducerRecord<String, String>> captor =
        ArgumentCaptor.forClass(ProducerRecord.class);

    verify(producer, times(1)).send(captor.capture());

    assertThat(captor.getValue().topic()).isEqualTo("test-topic");
    assertThat(captor.getValue().key()).isNull();
    assertThat(captor.getValue().value()).isEqualTo(OpenLineageClientUtils.toJson(event));
  }

  @Test
  void clientEmitsDatasetEventKafkaTransport() throws IOException {
    KafkaProducer<String, String> producer = mock(KafkaProducer.class);
    KafkaConfig config = new KafkaConfig();

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092;external:9092");

    config.setTopicName("test-topic");
    config.setProperties(properties);

    KafkaTransport transport = new KafkaTransport(producer, config);
    OpenLineageClient client = new OpenLineageClient(transport);

    when(producer.send(any(ProducerRecord.class))).thenReturn(mock(Future.class));

    OpenLineage.DatasetEvent event = datasetEvent();
    client.emit(event);

    ArgumentCaptor<ProducerRecord<String, String>> captor =
        ArgumentCaptor.forClass(ProducerRecord.class);

    verify(producer, times(1)).send(captor.capture());

    assertThat(captor.getValue().topic()).isEqualTo("test-topic");
    assertThat(captor.getValue().key()).isEqualTo("dataset:test-namespace/test-dataset");
    assertThat(captor.getValue().value()).isEqualTo(OpenLineageClientUtils.toJson(event));
  }

  @Test
  void clientEmitsJobEventKafkaTransport() throws IOException {
    KafkaProducer<String, String> producer = mock(KafkaProducer.class);
    KafkaConfig config = new KafkaConfig();

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092;external:9092");

    config.setTopicName("test-topic");
    config.setProperties(properties);

    KafkaTransport transport = new KafkaTransport(producer, config);
    OpenLineageClient client = new OpenLineageClient(transport);

    when(producer.send(any(ProducerRecord.class))).thenReturn(mock(Future.class));

    OpenLineage.JobEvent event = jobEvent();
    client.emit(event);

    ArgumentCaptor<ProducerRecord<String, String>> captor =
        ArgumentCaptor.forClass(ProducerRecord.class);

    verify(producer, times(1)).send(captor.capture());

    assertThat(captor.getValue().topic()).isEqualTo("test-topic");
    assertThat(captor.getValue().key()).isEqualTo("job:test-namespace/test-job");
    assertThat(captor.getValue().value()).isEqualTo(OpenLineageClientUtils.toJson(event));
  }
}
