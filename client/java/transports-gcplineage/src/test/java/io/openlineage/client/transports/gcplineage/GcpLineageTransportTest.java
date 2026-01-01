/*
/* Copyright 2018-2026 contributors to the OpenLineage project
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
import java.security.GeneralSecurityException;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.util.Base64;
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
      throws IOException, GeneralSecurityException {
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
  private Path createMockCredentialsFile(Path tempDir)
      throws IOException, GeneralSecurityException {
    KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
    keyGen.initialize(2048);
    PrivateKey privateKey = keyGen.generateKeyPair().getPrivate();
    String pemBody =
        Base64.getMimeEncoder(64, "\n".getBytes()).encodeToString(privateKey.getEncoded());
    String mockedPrivateKey =
        "-----BEGIN PRIVATE KEY-----\n" + pemBody + "\n-----END PRIVATE KEY-----\n";
    String mockedEscapedPrivateKey = mockedPrivateKey.replace("\n", "\\n");
    String mockCredentials =
        "{\n"
            + "  \"type\": \"service_account\",\n"
            + "  \"project_id\": \"mock-project-id\",\n"
            + "  \"private_key_id\": \"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\n"
            + "  \"private_key\": \""
            + mockedEscapedPrivateKey
            + "\",\n"
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
