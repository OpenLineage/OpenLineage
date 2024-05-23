/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import static io.openlineage.client.Events.datasetEvent;
import static io.openlineage.client.Events.jobEvent;
import static io.openlineage.client.Events.runEvent;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.UserRecord;
import com.google.common.util.concurrent.ListenableFuture;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.OpenLineageClientUtils;
import java.io.IOException;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class KinesisTransportTest {
  @Test
  void clientEmitsRunEventKinesisTransport() throws IOException {
    KinesisProducer producer = mock(KinesisProducer.class);
    KinesisConfig config = new KinesisConfig();

    Properties properties = new Properties();
    properties.setProperty("MinConnections", "1");

    config.setRegion("us-west-2");
    config.setRoleArn("test-role");
    config.setStreamName("test-stream");
    config.setProperties(properties);

    KinesisTransport transport = new KinesisTransport(producer, config);
    OpenLineageClient client = new OpenLineageClient(transport);

    when(producer.addUserRecord(any(UserRecord.class))).thenReturn(mock(ListenableFuture.class));

    OpenLineage.RunEvent event = runEvent();
    client.emit(event);

    ArgumentCaptor<UserRecord> captor = ArgumentCaptor.forClass(UserRecord.class);

    verify(producer, times(1)).addUserRecord(captor.capture());

    assertThat(captor.getValue().getStreamName()).isEqualTo("test-stream");
    assertThat(captor.getValue().getPartitionKey()).isEqualTo("run:test-namespace/test-job");

    String data = new String(captor.getValue().getData().array(), "UTF-8");
    String serialized = OpenLineageClientUtils.toJson(event);
    assertThat(data).isEqualTo(serialized);
  }

  @Test
  void clientEmitsDatasetEventKinesisTransport() throws IOException {
    KinesisProducer producer = mock(KinesisProducer.class);
    KinesisConfig config = new KinesisConfig();

    Properties properties = new Properties();
    properties.setProperty("MinConnections", "1");

    config.setRegion("us-west-2");
    config.setRoleArn("test-role");
    config.setStreamName("test-stream");
    config.setProperties(properties);

    KinesisTransport transport = new KinesisTransport(producer, config);
    OpenLineageClient client = new OpenLineageClient(transport);

    when(producer.addUserRecord(any(UserRecord.class))).thenReturn(mock(ListenableFuture.class));

    OpenLineage.DatasetEvent event = datasetEvent();
    client.emit(event);

    ArgumentCaptor<UserRecord> captor = ArgumentCaptor.forClass(UserRecord.class);

    verify(producer, times(1)).addUserRecord(captor.capture());

    assertThat(captor.getValue().getStreamName()).isEqualTo("test-stream");
    assertThat(captor.getValue().getPartitionKey())
        .isEqualTo("dataset:test-namespace/test-dataset");

    String data = new String(captor.getValue().getData().array(), "UTF-8");
    String serialized = OpenLineageClientUtils.toJson(event);
    assertThat(data).isEqualTo(serialized);
  }

  @Test
  void clientEmitsJobEventKinesisTransport() throws IOException {
    KinesisProducer producer = mock(KinesisProducer.class);
    KinesisConfig config = new KinesisConfig();

    Properties properties = new Properties();
    properties.setProperty("MinConnections", "1");

    config.setRegion("us-west-2");
    config.setRoleArn("test-role");
    config.setStreamName("test-stream");
    config.setProperties(properties);

    KinesisTransport transport = new KinesisTransport(producer, config);
    OpenLineageClient client = new OpenLineageClient(transport);

    when(producer.addUserRecord(any(UserRecord.class))).thenReturn(mock(ListenableFuture.class));

    OpenLineage.JobEvent event = jobEvent();
    client.emit(event);

    ArgumentCaptor<UserRecord> captor = ArgumentCaptor.forClass(UserRecord.class);

    verify(producer, times(1)).addUserRecord(captor.capture());

    assertThat(captor.getValue().getStreamName()).isEqualTo("test-stream");
    assertThat(captor.getValue().getPartitionKey()).isEqualTo("job:test-namespace/test-job");

    String data = new String(captor.getValue().getData().array(), "UTF-8");
    String serialized = OpenLineageClientUtils.toJson(event);
    assertThat(data).isEqualTo(serialized);
  }
}
