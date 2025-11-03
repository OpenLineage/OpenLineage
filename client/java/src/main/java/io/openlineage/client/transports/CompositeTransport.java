/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.BaseEvent;
import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.OpenLineageClientUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ToString
public class CompositeTransport extends Transport {

  private final CompositeConfig config;
  private final List<Transport> transports = new ArrayList<>();
  private final Optional<ExecutorService> executorService;

  public CompositeTransport(@NonNull CompositeConfig config) {
    this.config = config;
    initializeTransports();

    if (config.getWithThreadPool() && config.getContinueOnFailure()) {
      executorService = Optional.of(OpenLineageClientUtils.getOrCreateExecutor());
    } else {
      executorService = Optional.empty();
    }
  }

  private void initializeTransports() {
    for (TransportConfig transportConfig : config.getTransports()) {
      Transport transport = TransportResolver.resolveTransportByConfig(transportConfig);
      transports.add(transport);
    }
  }

  public List<Transport> getTransports() {
    return transports;
  }

  @Override
  public void emit(@NonNull OpenLineage.RunEvent runEvent) {
    doEmit(runEvent);
  }

  @Override
  public void emit(@NonNull OpenLineage.DatasetEvent datasetEvent) {
    doEmit(datasetEvent);
  }

  @Override
  public void emit(@NonNull OpenLineage.JobEvent jobEvent) {
    doEmit(jobEvent);
  }

  /**
   * Emit events in parallel using a thread pool.
   *
   * @param event
   */
  private void doEmit(BaseEvent event) {
    if (!config.getContinueOnFailure() || !config.getWithThreadPool()) {
      // Emit events sequentially
      for (Transport transport : transports) {
        emit(transport, event);
      }
    } else {
      try {
        executorService
            .get()
            .invokeAll(
                transports.stream()
                    .map(
                        t ->
                            (Callable<Void>)
                                () -> {
                                  emit(t, event);
                                  return null;
                                })
                    .collect(Collectors.toList()))
            .forEach(
                f -> {
                  try {
                    f.get();
                  } catch (InterruptedException | ExecutionException e) {
                    // do nothing, continue with the next transport
                    log.warn("One of the composite transports failed with: {}", e.getMessage());
                  }
                });
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * @param transport
   * @param event
   */
  private void emit(Transport transport, BaseEvent event) {
    try {
      if (event instanceof OpenLineage.RunEvent) {
        transport.emit((OpenLineage.RunEvent) event);
      } else if (event instanceof OpenLineage.DatasetEvent) {
        transport.emit((OpenLineage.DatasetEvent) event);
      } else if (event instanceof OpenLineage.JobEvent) {
        transport.emit((OpenLineage.JobEvent) event);
      } else {
        throw new IllegalArgumentException("Unsupported event type: " + event.getClass().getName());
      }
    } catch (Exception e) {
      // enrich exception with an information about the failing transport
      throw new RuntimeException(
          "Transport " + transport.getClass().getSimpleName() + " failed to emit event", e);
    }
  }

  @Override
  public void close() throws Exception {
    // do not close executor service as it is shared
    Exception latestException = null;
    for (Transport transport : transports) {
      try {
        transport.close();
      } catch (Exception e) {
        log.error("Failed to close {} transport", transport.getClass().getSimpleName(), e);
        latestException = e;
      }
    }
    if (latestException != null) {
      throw new OpenLineageClientException(latestException);
    }
  }
}
