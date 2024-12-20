/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.client.transports.gcplineage;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.datacatalog.lineage.v1.LineageSettings;
import com.google.cloud.datacatalog.lineage.v1.ProcessOpenLineageRunEventRequest;
import com.google.cloud.datacatalog.lineage.v1.ProcessOpenLineageRunEventResponse;
import com.google.cloud.datalineage.producerclient.helpers.OpenLineageHelper;
import com.google.cloud.datalineage.producerclient.v1.AsyncLineageClient;
import com.google.cloud.datalineage.producerclient.v1.AsyncLineageProducerClient;
import com.google.cloud.datalineage.producerclient.v1.AsyncLineageProducerClientSettings;
import com.google.cloud.datalineage.producerclient.v1.SyncLineageClient;
import com.google.cloud.datalineage.producerclient.v1.SyncLineageProducerClient;
import com.google.cloud.datalineage.producerclient.v1.SyncLineageProducerClientSettings;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.MoreExecutors;
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
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GcpLineageTransport extends Transport {

  private final ProducerClientWrapper producerClientWrapper;

  public GcpLineageTransport(@NonNull GcpLineageTransportConfig config) throws IOException {
    this(new ProducerClientWrapper(config));
  }

  protected GcpLineageTransport(@NonNull ProducerClientWrapper client) throws IOException {
    this.producerClientWrapper = client;
  }

  @Override
  public void emit(OpenLineage.@NonNull RunEvent runEvent) {
    producerClientWrapper.emitEvent(runEvent);
  }

  @Override
  public void emit(OpenLineage.@NonNull DatasetEvent datasetEvent) {
    producerClientWrapper.emitEvent(datasetEvent);
  }

  @Override
  public void emit(OpenLineage.@NonNull JobEvent jobEvent) {
    producerClientWrapper.emitEvent(jobEvent);
  }

  @Override
  public void close() {
    producerClientWrapper.close();
  }

  static class ProducerClientWrapper implements Closeable {
    private final SyncLineageClient syncLineageClient;
    private final AsyncLineageClient asyncLineageClient;
    private final String parent;

    protected ProducerClientWrapper(GcpLineageTransportConfig config) throws IOException {
      LineageSettings settings;
      if (GcpLineageTransportConfig.Mode.sync == config.getMode()) {
        settings = createSyncSettings(config);
        syncLineageClient =
            SyncLineageProducerClient.create((SyncLineageProducerClientSettings) settings);
        asyncLineageClient = null;
      } else {
        syncLineageClient = null;
        settings = createAsyncSettings(config);
        asyncLineageClient =
            AsyncLineageProducerClient.create((AsyncLineageProducerClientSettings) settings);
      }
      this.parent = getParent(config, settings);
    }

    protected ProducerClientWrapper(GcpLineageTransportConfig config, SyncLineageClient client)
        throws IOException {
      this.syncLineageClient = client;
      this.parent = getParent(config, createAsyncSettings(config));
      this.asyncLineageClient = null;
    }

    protected ProducerClientWrapper(GcpLineageTransportConfig config, AsyncLineageClient client)
        throws IOException {
      this.asyncLineageClient = client;
      this.parent = getParent(config, createSyncSettings(config));
      this.syncLineageClient = null;
    }

    public <T extends OpenLineage.BaseEvent> void emitEvent(T event) {
      try {
        String eventJson = OpenLineageClientUtils.toJson(event);
        Struct openLineageStruct = OpenLineageHelper.jsonToStruct(eventJson);
        ProcessOpenLineageRunEventRequest request =
            ProcessOpenLineageRunEventRequest.newBuilder()
                .setParent(parent)
                .setOpenLineage(openLineageStruct)
                .build();
        if (syncLineageClient != null) {
          syncLineageClient.processOpenLineageRunEvent(request);
        } else {
          handleRequestAsync(request);
        }
      } catch (Exception e) {
        throw new OpenLineageClientException(e);
      }
    }

    private void handleRequestAsync(ProcessOpenLineageRunEventRequest request) {
      ApiFuture<ProcessOpenLineageRunEventResponse> future =
          asyncLineageClient.processOpenLineageRunEvent(request);
      ApiFutureCallback<ProcessOpenLineageRunEventResponse> callback =
          new ApiFutureCallback<ProcessOpenLineageRunEventResponse>() {
            @Override
            public void onFailure(Throwable t) {
              log.error("Failed to collect a lineage event: {}", request.getOpenLineage(), t);
            }

            @Override
            public void onSuccess(ProcessOpenLineageRunEventResponse result) {
              log.debug("Event sent successfully: {}", request.getOpenLineage());
            }
          };
      ApiFutures.addCallback(future, callback, MoreExecutors.directExecutor());
    }

    private String getParent(GcpLineageTransportConfig config, LineageSettings settings)
        throws IOException {
      return String.format(
          "projects/%s/locations/%s",
          getProjectId(config, settings),
          config.getLocation() != null ? config.getLocation() : "us");
    }

    private static SyncLineageProducerClientSettings createSyncSettings(
        GcpLineageTransportConfig config) throws IOException {
      SyncLineageProducerClientSettings.Builder builder =
          SyncLineageProducerClientSettings.newBuilder();
      return createSettings(config, builder).build();
    }

    private static AsyncLineageProducerClientSettings createAsyncSettings(
        GcpLineageTransportConfig config) throws IOException {
      AsyncLineageProducerClientSettings.Builder builder =
          AsyncLineageProducerClientSettings.newBuilder();
      return createSettings(config, builder).build();
    }

    @VisibleForTesting
    static <T extends LineageSettings.Builder> T createSettings(
        GcpLineageTransportConfig config, T builder) throws IOException {
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
      getExtensions().forEach(extension -> extension.updateSettings(builder));
      return builder;
    }

    private static Iterable<GCPLineageTransportExtension> getExtensions() {
      ServiceLoader<GCPLineageTransportExtension> loader =
          ServiceLoader.load(GCPLineageTransportExtension.class);
      return StreamSupport.stream(loader.spliterator(), false).collect(Collectors.toList());
    }

    private static String getProjectId(GcpLineageTransportConfig config, LineageSettings settings)
        throws IOException {
      if (config.getProjectId() != null) {
        return config.getProjectId();
      }
      Credentials credentials = settings.getCredentialsProvider().getCredentials();
      if (credentials instanceof ServiceAccountCredentials) {
        ServiceAccountCredentials serviceAccountCredentials =
            (ServiceAccountCredentials) credentials;
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
        if (syncLineageClient != null) {
          syncLineageClient.close();
        }
        if (asyncLineageClient != null) {
          asyncLineageClient.close();
        }
      } catch (Exception e) {
        throw new OpenLineageClientException("Exception while closing the resource", e);
      }
    }
  }
}
