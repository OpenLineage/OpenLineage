/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports.kinesis;

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
import java.net.URI;
import java.util.Properties;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class KinesisTransportTest {
  @Test
  void clientEmitsRunEventKinesisTransport() throws IOException {
    KinesisProducer producer = Mockito.mock(KinesisProducer.class);
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

    OpenLineage.Job job =
        new OpenLineage.JobBuilder().namespace("test-namespace").name("test-job").build();
    OpenLineage.Run run =
        new OpenLineage.RunBuilder()
            .runId(UUID.fromString("ea445b5c-22eb-457a-8007-01c7c52b6e54"))
            .build();
    OpenLineage.RunEvent event =
        new OpenLineage(URI.create("http://test.producer"))
            .newRunEventBuilder()
            .job(job)
            .run(run)
            .build();
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
    KinesisProducer producer = Mockito.mock(KinesisProducer.class);
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

    OpenLineage.StaticDataset dataset =
        new OpenLineage.StaticDatasetBuilder()
            .namespace("test-namespace")
            .name("test-dataset")
            .build();
    OpenLineage.DatasetEvent event =
        new OpenLineage(URI.create("http://test.producer"))
            .newDatasetEventBuilder()
            .dataset(dataset)
            .build();
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
    KinesisProducer producer = Mockito.mock(KinesisProducer.class);
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

    OpenLineage.Job job =
        new OpenLineage.JobBuilder().namespace("test-namespace").name("test-job").build();
    OpenLineage.JobEvent event =
        new OpenLineage(URI.create("http://test.producer")).newJobEventBuilder().job(job).build();
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
