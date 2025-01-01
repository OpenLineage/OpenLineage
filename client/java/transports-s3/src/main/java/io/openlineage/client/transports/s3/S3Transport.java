/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports.s3;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.transports.Transport;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.utils.AttributeMap;

@Slf4j
public class S3Transport extends Transport {
  private static final String FILENAME_FORMAT_FOR_DIRECTORY_PREFIX = "%s%s.json";
  private static final String FILENAME_FORMAT_FOR_FILE_PREFIX = "%s_%s.json";

  private final S3Client s3Client;
  private final S3TransportConfig config;

  public S3Transport(S3TransportConfig config) {
    this.config = Objects.requireNonNull(config, "S3TransportConfig must not be null");
    this.s3Client = buildS3Client(config);
  }

  private static S3Client buildS3Client(S3TransportConfig config) {
    S3ClientBuilder builder = S3Client.builder();
    if (config.getEndpoint() != null) {
      builder.endpointOverride(URI.create(config.getEndpoint()));
    }
    return builder
        .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
        .httpClient(
            UrlConnectionHttpClient.builder()
                .buildWithDefaults(
                    AttributeMap.builder()
                        .put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true)
                        .build()))
        .build();
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

  private <T extends OpenLineage.BaseEvent> void emitEvent(T event) {
    Long timestamp = event.getEventTime().toInstant().toEpochMilli();
    uploadObject(getFileName(timestamp), OpenLineageClientUtils.toJson(event));
  }

  private String getFileName(Long timestamp) {
    if (config.getFileNamePrefix() != null) {
      String prefix = config.getFileNamePrefix();
      return prefix.endsWith("/")
          ? String.format(FILENAME_FORMAT_FOR_DIRECTORY_PREFIX, prefix, timestamp)
          : String.format(FILENAME_FORMAT_FOR_FILE_PREFIX, prefix, timestamp);
    } else {
      return timestamp + ".json";
    }
  }

  private void uploadObject(String objectName, String contents) {
    String bucketName = config.getBucketName();
    log.debug("Attempting to upload event to bucket: {} with key: {}", bucketName, objectName);
    // We don't want to overwrite the files
    if (objectExists(bucketName, objectName)) {
      String error =
          String.format("File with given name s3://%s/%s, already exists!", bucketName, objectName);
      throw new OpenLineageClientException(error);
    }
    s3Client.putObject(
        PutObjectRequest.builder().bucket(bucketName).key(objectName).build(),
        RequestBody.fromBytes(contents.getBytes(StandardCharsets.UTF_8)));
    log.debug("Stored event: {}/{}", bucketName, objectName);
  }

  private boolean objectExists(String bucketName, String objectName) {
    try {
      // Try to retrieve the metadata from an object without returning the object itself.
      s3Client.headObject(HeadObjectRequest.builder().bucket(bucketName).key(objectName).build());
      return true;
    } catch (NoSuchKeyException e) {
      return false;
    }
  }

  public void close() {
    s3Client.close();
  }

  // Visible for testing
  S3Client getS3Client() {
    return s3Client;
  }
}
