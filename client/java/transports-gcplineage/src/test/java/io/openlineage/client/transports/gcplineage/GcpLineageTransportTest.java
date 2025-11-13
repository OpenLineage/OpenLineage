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
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
  void testAsyncClientWithGracefulShutdownDuration(@TempDir Path tempDir) throws Exception {
    Path path = createMockCredentialsFile(tempDir);
    GcpLineageTransportConfig config = new GcpLineageTransportConfig();
    config.setProjectId("test-project");
    config.setLocation("asia-southeast1");
    config.setMode(GcpLineageTransportConfig.Mode.ASYNC);
    config.setGracefulShutdownDuration("PT1H30M45S");
    config.setCredentialsFile(path.toString());

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
  void testAsyncClientWithInvalidGracefulShutdownDuration(@TempDir Path tempDir)
      throws IOException {
    // Initialize config programmatically with invalid duration
    Path path = createMockCredentialsFile(tempDir);
    GcpLineageTransportConfig config = new GcpLineageTransportConfig();
    config.setProjectId("test-project");
    config.setLocation("us-central1");
    config.setMode(GcpLineageTransportConfig.Mode.ASYNC);
    config.setGracefulShutdownDuration("invalid-duration");
    config.setCredentialsFile(path.toString());

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

  @SuppressWarnings({"PMD.AvoidAccessibilityAlteration"})
  private <T, V> V getValue(String fieldName, Class<T> clazz, T object)
      throws NoSuchFieldException, IllegalAccessException {
    Field clientField = clazz.getDeclaredField(fieldName);
    clientField.setAccessible(true);
    return (V) clientField.get(object);
  }

  /**
   * Creates a mock GCP service account credentials file for testing purposes. This file contains
   * fake credentials that won't trigger GitHub's secret scanning.
   *
   * @param tempDir temporary directory provided by JUnit
   * @return Path to the created mock credentials file
   * @throws IOException if file creation fails
   */
  private Path createMockCredentialsFile(Path tempDir) throws IOException {
    String mockCredentials =
        "{\n"
            + "  \"type\": \"service_account\",\n"
            + "  \"project_id\": \"mock-project-id\",\n"
            + "  \"private_key_id\": \"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\n"
            + "  \"private_key\": \"-----BEGIN PRIVATE KEY-----\\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDCoz063WtcAljx\\nMaNw+2XNfDSPFOlj4iG+8ZeA94gGOv2DAY4h2s4gGhHixl0T0A0T+kxCwoVtnpu8\\naCbklDpN2thjodd/gEL49crxS6O1lbIx8C0ibbX+qnaD9Q3jYJ3jU+fDEJJQ/fah\\ndTsncRrSwXY4FtmOPjObexY2xW3J+56tszf4fYdSgfPdDZVkRD3D2jMVYxoi4kku\\nnU11CNKAqsm5WETpId8hUhkJRe5kqAVl9tJ4yAQWS3M8HUfgtRsHEpLF2jPGq685\\ntuy4OUwTJsIriIRiUkr96UE1ms6dBQoM9hJeGPJOuGyn0kJ9bBPrG75uK8XDaNDI\\nsFJ+YqTRAgMBAAECggEABbBTaE8wvCJkBUr/aZ5zJFUvsJvtSuZN9j+AA1WSWReC\\n0uZY6h+c4mfNqndILQC136ZA9w2isKCiI+f964V4zIh/RrcOnxPwM8L52DqwB7HL\\nC0lKdCpbuEx/AJuykaFpJhB1HL+YAtnhNOdeKmeZ4YL2Q4nVUKbeaoUUVx3X/sDw\\n5r9lo73WxW0ckYRF4hzICzhaTo28FuH/X0mWdAfAXcdB4EY5uHoMs+ZPTbwGqbwe\\nPXfNXs38lC/bwZWb3bSAP2zHAZJ1OqU6iATvDdB2pIQX40UZ+Ujp9bZKmfUmSX63\\nEnSeTh7dNmGZDiKRFeR50q7CsroTRl33zzvUvguhQQKBgQD209RcJSM3/dWLjPya\\nHIzyKIAx6j9+gsjW27Y/0FyPNJ97Mn1J86HoEekzR/LLUvhkQIrqO6STdIt5TCoF\\nhHqbYFPus1m43F5fgR5AEJAqgI9wUCLRuQqUnNQNNAVt3CKN5K2/4j/a59rh5HJg\\nMPPt9c8tgTFkjccn9S2xU5iiMQKBgQDI1WSuszqY0q+y786UrsFt8Z+0sfCD5JAU\\nmSUDsFAic6IuMkdT4hF3lylQpmf5xD+ptfHSDag3lcZut88Irgzut4tX/RmT+zXW\\nPqi4QYtSAaYTGdcuWMl/8rmVog0CqgxgHPSa6nL7nRKAk8qfV3JzwKsqy+8grmCz\\nfHOw9MHkoQKBgQCzhWolAtXUuYgBka9/n1hcIFzs8QTxTMoqi27IhxFrDskX36cE\\njHCry6sjIydR/qyurcrbhjmzDccLl/vQO4S5UZx6NnQBYjY5nD2WNvXEE/E/rOlG\\nRCGP6WjJmZaBSuTO8w30S+hJnOyz82XE1JX18xyWaiq0ifHZ/BcZrEWNYQKBgQCL\\nQcSNisOv4i9ocPYajM6dMLTf855lph/t2H8c/q2iJfIn/D8PQCuCdDN2s9xXCShn\\nwjyKvWOOH3G3pgaN6zoWcPjTKzIINWGQTGRrVy+GzpPcnMdjYLdf2+upgPNqjIUG\\nRC2sGbNfGvwQYepW8Kjw8ID/rOcED0YITtxdsGmd4QKBgHTgpiODyMkYdxFXgeDg\\n/qjhP5Jj5BEAHOIJ5E/ZubVpfaVqQQEVN9NjVIrT8UtxHPdnmi/yOHktTr9YSxqd\\n+6cBhSL0hv0+Nsr/+PdJEIo4Mw8nmx1Y8hljzFSeRL+eAnZGgD3mRxzS2TES5EIw\\nVr7mQM5JOlE8jknFvhyxBiSf\\n-----END PRIVATE KEY-----\\n\",\n"
            + "  \"client_email\": \"mock-email@gcp-open-lineage-testing.iam.gserviceaccount.com\",\n"
            + "  \"client_id\": \"101011010101111111001\",\n"
            + "  \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",\n"
            + "  \"token_uri\": \"https://oauth2.googleapis.com/token\",\n"
            + "  \"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\",\n"
            + "  \"client_x509_cert_url\": \"https://www.googleapis.com/robot/v1/metadata/x509/mock-email%40gcp-open-lineage-testing.iam.gserviceaccount.com\",\n"
            + "  \"universe_domain\": \"googleapis.com\"\n"
            + "}";

    Path credentialsFile = tempDir.resolve("mock-credentials.json");
    Files.write(credentialsFile, mockCredentials.getBytes(StandardCharsets.UTF_8));
    return credentialsFile;
  }
}
