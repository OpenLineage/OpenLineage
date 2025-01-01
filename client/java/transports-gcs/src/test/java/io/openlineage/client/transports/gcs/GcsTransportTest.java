/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports.gcs;

import static org.junit.jupiter.api.Assertions.*;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.OpenLineageClientUtils;
import java.net.URI;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class GcsTransportTest {

  private static final Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());

  @Test
  void shouldWriteEventToGcsWithNamePrefix() {
    Storage localStorage = LocalStorageHelper.getOptions().getService();
    GcsTransportConfig config =
        new GcsTransportConfig("test_project", "test_bucket", null, "some_prefix");
    GcsTransport transport = new GcsTransport(localStorage, config);
    OpenLineageClient client = new OpenLineageClient(transport);

    OpenLineage.RunEvent event = runEvent(clock);
    client.emit(event);

    Blob blob =
        localStorage.get(
            BlobId.of(
                "test_bucket",
                String.format(
                    "some_prefix_%s.json", ZonedDateTime.now(clock).toInstant().toEpochMilli())));
    assertNotNull(blob);
    assertEquals(new String(blob.getContent()), OpenLineageClientUtils.toJson(event));
  }

  @Test
  void shouldWriteEventToGcsWithPathPrefix() {
    Storage localStorage = LocalStorageHelper.getOptions().getService();
    GcsTransportConfig config =
        new GcsTransportConfig("test_project", "test_bucket", null, "some/prefix/");
    GcsTransport transport = new GcsTransport(localStorage, config);
    OpenLineageClient client = new OpenLineageClient(transport);

    OpenLineage.RunEvent event = runEvent(clock);
    client.emit(event);

    Blob blob =
        localStorage.get(
            BlobId.of(
                "test_bucket",
                String.format(
                    "some/prefix/%s.json", ZonedDateTime.now(clock).toInstant().toEpochMilli())));
    assertNotNull(blob);
    assertEquals(new String(blob.getContent()), OpenLineageClientUtils.toJson(event));
  }

  @Test
  void shouldThrowIfObjectAlreadyExists() {
    Storage localStorage = LocalStorageHelper.getOptions().getService();
    GcsTransportConfig config =
        new GcsTransportConfig("test_project", "test_bucket", null, "some/prefix/");
    GcsTransport transport = new GcsTransport(localStorage, config);
    OpenLineageClient client = new OpenLineageClient(transport);

    OpenLineage.RunEvent event = runEvent(clock);
    localStorage.create(
        BlobInfo.newBuilder(
                BlobId.of(
                    "test_bucket",
                    String.format(
                        "some/prefix/%s.json",
                        ZonedDateTime.now(clock).toInstant().toEpochMilli())))
            .build());
    assertThrows(OpenLineageClientException.class, () -> client.emit(event));
  }

  public static OpenLineage.RunEvent runEvent(Clock clock) {
    OpenLineage.Job job =
        new OpenLineage.JobBuilder().namespace("test-namespace").name("test-job").build();
    OpenLineage.Run run =
        new OpenLineage.RunBuilder()
            .runId(UUID.fromString("ea445b5c-22eb-457a-8007-01c7c52b6e54"))
            .build();
    return new OpenLineage(URI.create("http://test.producer"))
        .newRunEventBuilder()
        .eventTime(ZonedDateTime.now(clock))
        .job(job)
        .run(run)
        .build();
  }
}
