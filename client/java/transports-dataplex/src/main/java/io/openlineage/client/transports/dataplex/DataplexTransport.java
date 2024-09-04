/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.client.transports.dataplex;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.datacatalog.lineage.v1.LineageSettings;
import com.google.cloud.datacatalog.lineage.v1.ProcessOpenLineageRunEventRequest;
import com.google.cloud.datalineage.producerclient.helpers.OpenLineageHelper;
import com.google.cloud.datalineage.producerclient.v1.SyncLineageClient;
import com.google.cloud.datalineage.producerclient.v1.SyncLineageProducerClient;
import com.google.cloud.datalineage.producerclient.v1.SyncLineageProducerClientSettings;
import com.google.protobuf.Struct;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.transports.Transport;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataplexTransport extends Transport implements Closeable {

  private final SyncLineageClient producerClient;

  private final String parent;

  public DataplexTransport(@NonNull DataplexConfig config) throws IOException {
    this(config, SyncLineageProducerClient.create(createSettings(config)));
  }

  protected DataplexTransport(@NonNull DataplexConfig config, @NonNull SyncLineageClient client)
      throws IOException {
    this.producerClient = client;
    this.parent =
        String.format(
            "projects/%s/locations/%s",
            getProjectId(config, createSettings(config)),
            config.getLocations() != null ? config.getLocations() : "us");
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
    try {
      String eventJson = OpenLineageClientUtils.toJson(event);
      Struct openLineageStruct = OpenLineageHelper.jsonToStruct(eventJson);
      ProcessOpenLineageRunEventRequest request =
          ProcessOpenLineageRunEventRequest.newBuilder()
              .setParent(parent)
              .setOpenLineage(openLineageStruct)
              .build();
      producerClient.processOpenLineageRunEvent(request);
    } catch (Exception e) {
      throw new OpenLineageClientException(e);
    }
  }

  private static SyncLineageProducerClientSettings createSettings(DataplexConfig config)
      throws IOException {
    SyncLineageProducerClientSettings.Builder builder =
        SyncLineageProducerClientSettings.newBuilder();

    if (config.getEndpoint() != null) {
      builder.setEndpoint(config.getEndpoint());
    }
    if (config.getProjectId() != null) {
      builder.setQuotaProjectId(config.getProjectId());
    }
    if (config.getCredentialsFile() != null) {
      File file = new File(config.getCredentialsFile());
      try (InputStream credentialsStream = Files.newInputStream(file.toPath())) {
        GoogleCredentials googleCredentials = GoogleCredentials.fromStream(credentialsStream);
        builder.setCredentialsProvider(FixedCredentialsProvider.create(googleCredentials));
      }
    }
    if (config.getHeaders() != null && !config.getHeaders().isEmpty()) {
      builder.setHeaderProvider(FixedHeaderProvider.create(config.getHeaders()));
    }

    return builder.build();
  }

  private static <T extends LineageSettings> String getProjectId(DataplexConfig config, T settings)
      throws IOException {
    if (config.getProjectId() != null) {
      return config.getProjectId();
    }
    Credentials credentials = settings.getCredentialsProvider().getCredentials();
    if (credentials instanceof ServiceAccountCredentials) {
      ServiceAccountCredentials serviceAccountCredentials = (ServiceAccountCredentials) credentials;
      return serviceAccountCredentials.getProjectId();
    }
    if (credentials instanceof GoogleCredentials) {
      GoogleCredentials googleCredentials = (GoogleCredentials) credentials;
      return googleCredentials.getQuotaProjectId();
    }
    return settings.getQuotaProjectId();
  }

  @Override
  public void close() {
    try {
      producerClient.close();
    } catch (Exception e) {
      throw new OpenLineageClientException("Exception while closing the resource", e);
    }
  }
}
