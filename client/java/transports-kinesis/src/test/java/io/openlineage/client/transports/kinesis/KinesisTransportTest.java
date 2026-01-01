/*
/* Copyright 2018-2026 contributors to the OpenLineage project
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
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
  void clientEmitsRunEventWithParentFacetKinesisTransport() throws IOException {
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

    OpenLineage openLineage = new OpenLineage(URI.create("http://test.producer"));
    OpenLineage.ParentRunFacetJob parentJob =
        new OpenLineage.ParentRunFacetJobBuilder()
            .namespace("parent-namespace")
            .name("parent-job")
            .build();
    OpenLineage.ParentRunFacetRun parentRun =
        new OpenLineage.ParentRunFacetRunBuilder()
            .runId(UUID.fromString("d9cb8e0b-a410-435e-a619-da5e87ba8508"))
            .build();
    OpenLineage.ParentRunFacet parentRunFacet =
        openLineage.newParentRunFacetBuilder().job(parentJob).run(parentRun).build();
    OpenLineage.RunFacets runFacets =
        new OpenLineage.RunFacetsBuilder().parent(parentRunFacet).build();

    OpenLineage.Job job =
        new OpenLineage.JobBuilder().namespace("test-namespace").name("test-job").build();
    OpenLineage.Run run =
        new OpenLineage.RunBuilder()
            .runId(UUID.fromString("ea445b5c-22eb-457a-8007-01c7c52b6e54"))
            .facets(runFacets)
            .build();
    OpenLineage.RunEvent event = openLineage.newRunEventBuilder().job(job).run(run).build();
    client.emit(event);

    ArgumentCaptor<UserRecord> captor = ArgumentCaptor.forClass(UserRecord.class);

    verify(producer, times(1)).addUserRecord(captor.capture());

    assertThat(captor.getValue().getStreamName()).isEqualTo("test-stream");
    assertThat(captor.getValue().getPartitionKey()).isEqualTo("run:parent-namespace/parent-job");

    String data = new String(captor.getValue().getData().array(), "UTF-8");
    String serialized = OpenLineageClientUtils.toJson(event);
    assertThat(data).isEqualTo(serialized);
  }

  @Test
  void clientEmitsRunEventWithRootParentFacetKinesisTransport() throws IOException {
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

    OpenLineage.RootJob rootJob =
        new OpenLineage.RootJobBuilder().namespace("root-namespace").name("root-job").build();
    OpenLineage.RootRun rootRun =
        new OpenLineage.RootRunBuilder()
            .runId(UUID.fromString("ad0995e1-49f1-432d-92b6-2e2739034c13"))
            .build();
    OpenLineage.ParentRunFacetRoot rootParent =
        new OpenLineage.ParentRunFacetRootBuilder().job(rootJob).run(rootRun).build();

    OpenLineage openLineage = new OpenLineage(URI.create("http://test.producer"));
    OpenLineage.ParentRunFacetJob parentJob =
        new OpenLineage.ParentRunFacetJobBuilder()
            .namespace("parent-namespace")
            .name("parent-job")
            .build();
    OpenLineage.ParentRunFacetRun parentRun =
        new OpenLineage.ParentRunFacetRunBuilder()
            .runId(UUID.fromString("d9cb8e0b-a410-435e-a619-da5e87ba8508"))
            .build();
    OpenLineage.ParentRunFacet parentRunFacet =
        openLineage
            .newParentRunFacetBuilder()
            .job(parentJob)
            .run(parentRun)
            .root(rootParent)
            .build();
    OpenLineage.RunFacets runFacets =
        new OpenLineage.RunFacetsBuilder().parent(parentRunFacet).build();

    OpenLineage.Job job =
        new OpenLineage.JobBuilder().namespace("test-namespace").name("test-job").build();
    OpenLineage.Run run =
        new OpenLineage.RunBuilder()
            .runId(UUID.fromString("ea445b5c-22eb-457a-8007-01c7c52b6e54"))
            .facets(runFacets)
            .build();
    OpenLineage.RunEvent event = openLineage.newRunEventBuilder().job(job).run(run).build();
    client.emit(event);

    ArgumentCaptor<UserRecord> captor = ArgumentCaptor.forClass(UserRecord.class);

    verify(producer, times(1)).addUserRecord(captor.capture());

    assertThat(captor.getValue().getStreamName()).isEqualTo("test-stream");
    assertThat(captor.getValue().getPartitionKey()).isEqualTo("run:root-namespace/root-job");

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

  @Test
  void clientClose() throws Exception {
    KinesisProducer producer = Mockito.mock(KinesisProducer.class);
    KinesisConfig config = new KinesisConfig();

    Properties properties = new Properties();
    properties.setProperty("MinConnections", "1");

    config.setRegion("us-west-2");
    config.setRoleArn("test-role");
    config.setStreamName("test-stream");
    config.setProperties(properties);

    try (MockedStatic<Executors> factory = mockStatic(Executors.class)) {
      ExecutorService executor = mock(ExecutorService.class);
      when(Executors.newSingleThreadExecutor()).thenReturn(executor);

      KinesisTransport transport = new KinesisTransport(producer, config);
      transport.close();

      verify(producer, times(1)).flushSync();
      verify(executor, times(1)).shutdown();
    }
  }
}
