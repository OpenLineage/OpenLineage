/*
/* Copyright 2018-2025 contributors to the OpenLineage project
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Set;
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

  /**
   * Flush pending async operations by waiting for them to complete.
   *
   * @param timeoutSeconds the maximum time to wait in seconds
   * @return true if all operations completed within timeout, false otherwise
   */
  public boolean flush(long timeoutSeconds) {
    return producerClientWrapper.flush(timeoutSeconds);
  }

  /**
   * Flush with default timeout of 30 seconds.
   */
  public boolean flush() {
    return producerClientWrapper.flush();
  }

  @Override
  public void close() {
    producerClientWrapper.close();
  }

  static class ProducerClientWrapper implements Closeable {
    private final SyncLineageClient syncLineageClient;
    private final AsyncLineageClient asyncLineageClient;
    private final String parent;
    private final Set<ApiFuture<ProcessOpenLineageRunEventResponse>> pendingOperations;
    private final AtomicLong completedOperations;
    private volatile boolean closed = false;

    protected ProducerClientWrapper(GcpLineageTransportConfig config) throws IOException {
      this.pendingOperations = ConcurrentHashMap.newKeySet();
      this.completedOperations = new AtomicLong(0);
      
      LineageSettings settings;
      if (GcpLineageTransportConfig.Mode.SYNC == config.getMode()) {
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
      this.pendingOperations = ConcurrentHashMap.newKeySet();
      this.completedOperations = new AtomicLong(0);
      this.syncLineageClient = client;
      this.parent = getParent(config, createAsyncSettings(config));
      this.asyncLineageClient = null;
    }

    protected ProducerClientWrapper(GcpLineageTransportConfig config, AsyncLineageClient client)
        throws IOException {
      this.pendingOperations = ConcurrentHashMap.newKeySet();
      this.completedOperations = new AtomicLong(0);
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
      if (closed) {
        log.warn("Transport is closed, rejecting event emission");
        return;
      }
      
      ApiFuture<ProcessOpenLineageRunEventResponse> future =
          asyncLineageClient.processOpenLineageRunEvent(request);
      
      // Add future to tracking set for graceful shutdown
      pendingOperations.add(future);
      
      ApiFutureCallback<ProcessOpenLineageRunEventResponse> callback =
          new ApiFutureCallback<ProcessOpenLineageRunEventResponse>() {
            @Override
            public void onFailure(Throwable t) {
              pendingOperations.remove(future);
              completedOperations.incrementAndGet();
              log.error("Failed to collect a lineage event: {}", request.getOpenLineage(), t);
            }

            @Override
            public void onSuccess(ProcessOpenLineageRunEventResponse result) {
              pendingOperations.remove(future);
              completedOperations.incrementAndGet();
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

    private static <T extends LineageSettings.Builder> T createSettings(
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
      return builder;
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

    /**
     * Flush pending async operations by waiting for them to complete.
     *
     * @param timeoutSeconds the maximum time to wait in seconds (default: 30)
     * @return true if all operations completed within timeout, false otherwise
     */
    public boolean flush(long timeoutSeconds) {
      if (syncLineageClient != null) {
        // Sync client has no pending operations
        return true;
      }

      if (pendingOperations.isEmpty()) {
        log.debug("No pending operations to flush");
        return true;
      }

      long startTime = System.currentTimeMillis();
      long timeoutMs = timeoutSeconds * 1000;
      
      log.info("Flushing {} pending async operations (timeout: {}s)", 
               pendingOperations.size(), timeoutSeconds);

      while (!pendingOperations.isEmpty() && 
             (System.currentTimeMillis() - startTime) < timeoutMs) {
        try {
          Thread.sleep(100); // Check every 100ms
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          log.warn("Flush interrupted, {} operations may not complete", pendingOperations.size());
          return false;
        }
      }

      boolean allCompleted = pendingOperations.isEmpty();
      if (allCompleted) {
        log.info("All async operations completed successfully ({} total)", 
                 completedOperations.get());
      } else {
        log.warn("Flush timeout: {} operations still pending after {}s", 
                 pendingOperations.size(), timeoutSeconds);
      }
      
      return allCompleted;
    }

    /**
     * Flush with default timeout of 30 seconds.
     */
    public boolean flush() {
      return flush(30);
    }

    @Override
    public void close() {
      try {
        // Set closed flag to prevent new operations
        closed = true;
        
        // Flush pending async operations before closing
        if (asyncLineageClient != null && !pendingOperations.isEmpty()) {
          log.info("Flushing {} pending operations before shutdown", pendingOperations.size());
          boolean flushed = flush(30); // 30 second timeout
          if (!flushed) {
            log.warn("Some async operations may not have completed before shutdown");
          }
        }
        
        // Close clients
        if (syncLineageClient != null) {
          syncLineageClient.close();
        }
        if (asyncLineageClient != null) {
          asyncLineageClient.close();
        }
        
        // Clear any remaining operations
        pendingOperations.clear();
        
      } catch (Exception e) {
        throw new OpenLineageClientException("Exception while closing the resource", e);
      }
    }
  }
}
