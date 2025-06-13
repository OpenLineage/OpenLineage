/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.client.transports.gcplineage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFuture;
import com.google.api.gax.rpc.AsyncTaskException;
import com.google.cloud.datacatalog.lineage.v1.ProcessOpenLineageRunEventRequest;
import com.google.cloud.datacatalog.lineage.v1.ProcessOpenLineageRunEventResponse;
import com.google.cloud.datalineage.producerclient.helpers.OpenLineageHelper;
import com.google.cloud.datalineage.producerclient.v1.AsyncLineageProducerClient;
import com.google.cloud.datalineage.producerclient.v1.SyncLineageProducerClient;
import datalineage.shaded.org.threeten.bp.Duration;
import datalineage.shaded.org.threeten.bp.format.DateTimeParseException;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.transports.Transport;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class GcpLineageTransportTest {

  @Test
  void clientEmitsRunEventGCPTransportSyncMode() throws Exception {
    SyncLineageProducerClient syncClient = mock(SyncLineageProducerClient.class);
    GcpLineageTransportConfig config = new GcpLineageTransportConfig();

    config.setProjectId("my-project");
    config.setLocation("us");

    GcpLineageTransport.ProducerClientWrapper clientWrapper =
        new GcpLineageTransport.ProducerClientWrapper(config, syncClient);

    Transport transport = new GcpLineageTransport(clientWrapper);
    OpenLineageClient client = new OpenLineageClient(transport);

    OpenLineage.RunEvent event = runEvent();
    ProcessOpenLineageRunEventRequest request =
        ProcessOpenLineageRunEventRequest.newBuilder()
            .setParent("projects/my-project/locations/us")
            .setOpenLineage(OpenLineageHelper.jsonToStruct(OpenLineageClientUtils.toJson(event)))
            .build();
    when(syncClient.processOpenLineageRunEvent(request))
        .thenReturn(ProcessOpenLineageRunEventResponse.newBuilder().build());

    client.emit(event);

    verify(syncClient, times(1)).processOpenLineageRunEvent(request);
  }

  @Test
  void clientEmitsRunEventGCPTransportAsyncMode() throws Exception {
    AsyncLineageProducerClient asyncClient = mock(AsyncLineageProducerClient.class);
    GcpLineageTransportConfig config = new GcpLineageTransportConfig();

    config.setProjectId("my-project");
    config.setLocation("us");

    GcpLineageTransport.ProducerClientWrapper clientWrapper =
        new GcpLineageTransport.ProducerClientWrapper(config, asyncClient);

    Transport transport = new GcpLineageTransport(clientWrapper);
    OpenLineageClient client = new OpenLineageClient(transport);

    OpenLineage.RunEvent event = runEvent();
    ProcessOpenLineageRunEventRequest request =
        ProcessOpenLineageRunEventRequest.newBuilder()
            .setParent("projects/my-project/locations/us")
            .setOpenLineage(OpenLineageHelper.jsonToStruct(OpenLineageClientUtils.toJson(event)))
            .build();
    when(asyncClient.processOpenLineageRunEvent(request)).thenReturn(mock(ApiFuture.class));

    client.emit(event);

    verify(asyncClient, times(1)).processOpenLineageRunEvent(request);
  }

  @Test
  void gcpTransportRaisesOnException() throws Exception {
    AsyncLineageProducerClient async = mock(AsyncLineageProducerClient.class);
    GcpLineageTransportConfig config = new GcpLineageTransportConfig();
    config.setProjectId("my-project");
    config.setLocation("us");

    GcpLineageTransport.ProducerClientWrapper clientWrapper =
        new GcpLineageTransport.ProducerClientWrapper(config, async);

    Transport transport = new GcpLineageTransport(clientWrapper);
    OpenLineageClient client = new OpenLineageClient(transport);

    OpenLineage.RunEvent event = runEvent();
    ProcessOpenLineageRunEventRequest request =
        ProcessOpenLineageRunEventRequest.newBuilder()
            .setParent("projects/my-project/locations/us")
            .setOpenLineage(OpenLineageHelper.jsonToStruct(OpenLineageClientUtils.toJson(event)))
            .build();
    when(async.processOpenLineageRunEvent(request)).thenThrow(AsyncTaskException.class);

    assertThrows(OpenLineageClientException.class, () -> client.emit(runEvent()));
  }

  @SuppressWarnings({"unchecked", "PMD.AvoidAccessibilityAlteration"})
  @Test
  void testAsyncClientWithGracefulShutdownDuration() throws Exception {
    GcpLineageTransportConfig config = new GcpLineageTransportConfig();
    config.setProjectId("test-project");
    config.setLocation("asia-southeast1");
    config.setMode(GcpLineageTransportConfig.Mode.ASYNC);
    config.setGracefulShutdownDuration("PT1H30M45S");

    GcpLineageTransport gcpLineageTransport = new GcpLineageTransport(config);
    GcpLineageTransport.ProducerClientWrapper wrapper =
        getValue("producerClientWrapper", GcpLineageTransport.class, gcpLineageTransport);
    AsyncLineageProducerClient client =
        getValue("asyncLineageClient", GcpLineageTransport.ProducerClientWrapper.class, wrapper);
    Duration actualDuration =
        getValue("gracefulShutdownDuration", AsyncLineageProducerClient.class, client);

    Duration expectedDuration = Duration.ofHours(1).plusMinutes(30).plusSeconds(45);
    assertEquals(expectedDuration, actualDuration);
    assertNotNull(wrapper);
  }

  @Test
  void testAsyncClientWithInvalidGracefulShutdownDuration() {
    // Initialize config programmatically with invalid duration
    GcpLineageTransportConfig config = new GcpLineageTransportConfig();
    config.setProjectId("test-project");
    config.setLocation("us-central1");
    config.setMode(GcpLineageTransportConfig.Mode.ASYNC);
    config.setGracefulShutdownDuration("invalid-duration");

    assertThrows(
        DateTimeParseException.class,
        () -> {
          new GcpLineageTransport.ProducerClientWrapper(config);
        });
  }

  public static OpenLineage.RunEvent runEvent() {
    OpenLineage.Job job =
        new OpenLineage.JobBuilder().namespace("test-namespace").name("test-job").build();
    OpenLineage.Run run =
        new OpenLineage.RunBuilder()
            .runId(UUID.fromString("ea445b5c-22eb-457a-8007-01c7c52b6e54"))
            .build();
    return new OpenLineage(URI.create("http://test.producer"))
        .newRunEventBuilder()
        .job(job)
        .run(run)
        .build();
  }

  private <T, V> V getValue(String fieldName, Class<T> clazz, T object)
      throws NoSuchFieldException, IllegalAccessException {
    Field clientField = clazz.getDeclaredField(fieldName);
    clientField.setAccessible(true);
    return (V) clientField.get(object);
  }
}
