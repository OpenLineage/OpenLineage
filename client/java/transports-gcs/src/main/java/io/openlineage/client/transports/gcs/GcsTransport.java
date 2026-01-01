/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports.gcs;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.transports.Transport;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Optional;
import java.util.function.Function;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GcsTransport extends Transport {

  private final Storage storage;
  private final String bucketName;
  private final Optional<String> fileNamePrefix;

  public GcsTransport(GcsTransportConfig config) throws IOException {
    this(buildStorage(config), config);
  }

  public GcsTransport(Storage storage, GcsTransportConfig config) {
    this.storage = storage;
    this.bucketName = config.getBucketName();
    this.fileNamePrefix = Optional.ofNullable(config.getFileNamePrefix());
  }

  @Override
  public void emit(OpenLineage.@NonNull RunEvent runEvent) {
    emitEvent(runEvent);
  }

  @Override
  public void emit(OpenLineage.@NonNull DatasetEvent datasetEvent) {
    emitEvent(datasetEvent);
  }

  @Override
  public void emit(OpenLineage.@NonNull JobEvent jobEvent) {
    emitEvent(jobEvent);
  }

  private static Storage buildStorage(GcsTransportConfig config) throws IOException {
    StorageOptions.Builder builder = StorageOptions.newBuilder();
    builder.setProjectId(config.getProjectId());
    if (config.getCredentialsFile() != null) {
      File file = new File(config.getCredentialsFile());
      try (InputStream credentialsStream = Files.newInputStream(file.toPath())) {
        GoogleCredentials googleCredentials = GoogleCredentials.fromStream(credentialsStream);
        builder.setCredentials(googleCredentials);
      }
    }

    return builder.build().getService();
  }

  private <T extends OpenLineage.BaseEvent> void emitEvent(T event) {
    Long timestamp = event.getEventTime().toInstant().toEpochMilli();
    String fileName = fileNamePrefix.map(getFileName(timestamp)).orElse(timestamp + ".json");
    uploadObject(fileName, OpenLineageClientUtils.toJson(event));
  }

  private static Function<String, String> getFileName(Long timestamp) {
    return prefix ->
        prefix.endsWith("/")
            ? String.format("%s%s.json", prefix, timestamp)
            : String.format("%s_%s.json", prefix, timestamp);
  }

  private void uploadObject(String objectName, String contents) {

    BlobId blobId = BlobId.of(bucketName, objectName);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
    byte[] content = contents.getBytes(StandardCharsets.UTF_8);
    if (storage.get(bucketName, objectName) != null) {
      String error =
          String.format("File with given name %s, already exists!", blobId.toGsUtilUri());
      throw new OpenLineageClientException(error);
    }
    Blob blob = storage.create(blobInfo, content);
    log.debug("Stored event: {}", blob.asBlobInfo().getBlobId().toGsUtilUri());
  }

  @Override
  public void close() throws Exception {
    storage.close();
  }
}
