/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.client.transports.dataplex;

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
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.transports.Transport;
import java.net.URI;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class DataplexTransportTest {

  @Test
  void clientEmitsRunEventGCPTransportSyncMode() throws Exception {
    SyncLineageProducerClient syncClient = mock(SyncLineageProducerClient.class);
    DataplexConfig config = new DataplexConfig();

    config.setProjectId("my-project");
    config.setLocation("us");

    DataplexTransport.ProducerClientWrapper clientWrapper =
        new DataplexTransport.ProducerClientWrapper(config, syncClient);

    Transport transport = new DataplexTransport(clientWrapper);
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
    DataplexConfig config = new DataplexConfig();

    config.setProjectId("my-project");
    config.setLocation("us");

    DataplexTransport.ProducerClientWrapper clientWrapper =
        new DataplexTransport.ProducerClientWrapper(config, asyncClient);

    Transport transport = new DataplexTransport(clientWrapper);
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
    DataplexConfig config = new DataplexConfig();
    config.setProjectId("my-project");
    config.setLocation("us");

    DataplexTransport.ProducerClientWrapper clientWrapper =
        new DataplexTransport.ProducerClientWrapper(config, async);

    Transport transport = new DataplexTransport(clientWrapper);
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
}
