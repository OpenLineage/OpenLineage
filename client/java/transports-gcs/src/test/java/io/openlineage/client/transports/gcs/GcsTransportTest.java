/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports.gcs;

import static org.junit.jupiter.api.Assertions.*;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.OpenLineageClientUtils;
import java.net.URI;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.Test;

class GcsTransportTest {

  private static final Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
  private static final String HASH_REGEX = "[0-9a-f]{16}";

  @Test
  void shouldWriteEventToGcsWithNamePrefix() {
    Storage localStorage = LocalStorageHelper.getOptions().getService();
    OpenLineage.RunEvent event = runEvent(clock);
    emit(localStorage, "some_prefix", event);

    Blob blob = singleBlob(localStorage);
    assertTrue(
        blob.getName().matches("some_prefix_" + epochMillis() + "-" + HASH_REGEX + "\\.json"));
    assertEquals(new String(blob.getContent()), OpenLineageClientUtils.toJson(event));
  }

  @Test
  void shouldWriteEventToGcsWithPathPrefix() {
    Storage localStorage = LocalStorageHelper.getOptions().getService();
    OpenLineage.RunEvent event = runEvent(clock);
    emit(localStorage, "some/prefix/", event);

    Blob blob = singleBlob(localStorage);
    assertTrue(
        blob.getName().matches("some/prefix/" + epochMillis() + "-" + HASH_REGEX + "\\.json"));
    assertEquals(new String(blob.getContent()), OpenLineageClientUtils.toJson(event));
  }

  @Test
  void shouldWriteEventToGcsWithoutPrefix() {
    Storage localStorage = LocalStorageHelper.getOptions().getService();
    OpenLineage.RunEvent event = runEvent(clock);
    emit(localStorage, null, event);

    Blob blob = singleBlob(localStorage);
    assertTrue(blob.getName().matches(epochMillis() + "-" + HASH_REGEX + "\\.json"));
    assertEquals(new String(blob.getContent()), OpenLineageClientUtils.toJson(event));
  }

  @Test
  void shouldStoreAllEventsWithTheSameEventTime() {
    Storage localStorage = LocalStorageHelper.getOptions().getService();
    OpenLineage.RunEvent start = runEvent(clock, OpenLineage.RunEvent.EventType.START);
    OpenLineage.RunEvent complete = runEvent(clock, OpenLineage.RunEvent.EventType.COMPLETE);
    emit(localStorage, "some/prefix/", start, complete);

    List<String> contents =
        StreamSupport.stream(localStorage.list("test_bucket").iterateAll().spliterator(), false)
            .map(blob -> new String(blob.getContent()))
            .collect(Collectors.toList());
    assertEquals(2, contents.size());
    assertTrue(contents.contains(OpenLineageClientUtils.toJson(start)));
    assertTrue(contents.contains(OpenLineageClientUtils.toJson(complete)));
  }

  @Test
  void shouldStoreIdenticalEventOnce() {
    Storage localStorage = LocalStorageHelper.getOptions().getService();
    OpenLineage.RunEvent event = runEvent(clock);
    emit(localStorage, "some/prefix/", event, event);

    Blob blob = singleBlob(localStorage);
    assertEquals(new String(blob.getContent()), OpenLineageClientUtils.toJson(event));
  }

  @Test
  void shouldConfigureRetrySettings() throws Exception {
    GcsTransportConfig config = new GcsTransportConfig();
    config.setProjectId("test_project");
    config.setBucketName("test_bucket");
    config.setMaxRetries(3);
    config.setRetryIntervalMillis(2500);

    GcsTransport transport = new GcsTransport(config);
    RetrySettings retrySettings = transport.getStorage().getOptions().getRetrySettings();

    assertEquals(3, retrySettings.getMaxAttempts());
    assertEquals(2500L, retrySettings.getInitialRetryDelayDuration().toMillis());
    assertEquals(2500L, retrySettings.getMaxRetryDelayDuration().toMillis());
  }

  private static void emit(Storage storage, String prefix, OpenLineage.RunEvent... events) {
    GcsTransportConfig config =
        new GcsTransportConfig("test_project", "test_bucket", null, prefix, null, null);
    OpenLineageClient client = new OpenLineageClient(new GcsTransport(storage, config));
    for (OpenLineage.RunEvent event : events) {
      client.emit(event);
    }
  }

  private static Blob singleBlob(Storage storage) {
    List<Blob> blobs =
        StreamSupport.stream(storage.list("test_bucket").iterateAll().spliterator(), false)
            .collect(Collectors.toList());
    assertEquals(1, blobs.size());
    return storage.get(blobs.get(0).getBlobId());
  }

  private static long epochMillis() {
    return ZonedDateTime.now(clock).toInstant().toEpochMilli();
  }

  public static OpenLineage.RunEvent runEvent(Clock clock) {
    return runEvent(clock, OpenLineage.RunEvent.EventType.START);
  }

  public static OpenLineage.RunEvent runEvent(
      Clock clock, OpenLineage.RunEvent.EventType eventType) {
    OpenLineage.Job job =
        new OpenLineage.JobBuilder().namespace("test-namespace").name("test-job").build();
    OpenLineage.Run run =
        new OpenLineage.RunBuilder()
            .runId(UUID.fromString("ea445b5c-22eb-457a-8007-01c7c52b6e54"))
            .build();
    return new OpenLineage(URI.create("http://test.producer"))
        .newRunEventBuilder()
        .eventTime(ZonedDateTime.now(clock))
        .eventType(eventType)
        .job(job)
        .run(run)
        .build();
  }
}
