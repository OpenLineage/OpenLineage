/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientException;
import java.util.ArrayList;
import java.util.List;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CompositeTransport extends Transport {

  private final CompositeConfig config;
  private final List<Transport> transports = new ArrayList<>();

  public CompositeTransport(@NonNull CompositeConfig config) {
    this.config = config;
    initializeTransports();
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
    for (Transport transport : transports) {
      try {
        transport.emit(runEvent);
      } catch (Exception e) {
        handleEmissionFailure(transport, e);
      }
    }
  }

  @Override
  public void emit(@NonNull OpenLineage.DatasetEvent datasetEvent) {
    for (Transport transport : transports) {
      try {
        transport.emit(datasetEvent);
      } catch (Exception e) {
        handleEmissionFailure(transport, e);
      }
    }
  }

  @Override
  public void emit(@NonNull OpenLineage.JobEvent jobEvent) {
    for (Transport transport : transports) {
      try {
        transport.emit(jobEvent);
      } catch (Exception e) {
        handleEmissionFailure(transport, e);
      }
    }
  }

  private void handleEmissionFailure(Transport transport, Exception e) {
    if (!config.isContinueOnFailure()) {
      throw new RuntimeException(
          "Transport " + transport.getClass().getSimpleName() + " failed to emit event", e);
    }
  }

  @Override
  public void close() throws Exception {
    transports.forEach(
        t -> {
          try {
            t.close();
          } catch (Exception e) {
            log.error("Failed to close {} transport", t.getClass().getSimpleName(), e);
            throw new OpenLineageClientException(e);
          }
        });
  }
}
