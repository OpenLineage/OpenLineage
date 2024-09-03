/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.client.OpenLineage;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CompositeTransport extends Transport {

  private final CompositeConfig config;
  private final List<Transport> transports = new ArrayList<>();

  public CompositeTransport(@NonNull CompositeConfig config) {
    super(Type.NOOP); // Type doesn't matter for CompositeTransport
    this.config = config;
    initializeTransports();
  }

  private void initializeTransports() {
    for (Map<String, Object> transportParams : config.getTransports()) {
      TransportConfig transportConfig;
      try {
        transportConfig = createTransportConfig(transportParams);
      } catch (JsonProcessingException e) {
        throw new RuntimeException("Error creating transport config", e);
      }
      Transport transport = TransportResolver.resolveTransportByConfig(transportConfig);
      transports.add(transport);
    }
  }

  private TransportConfig createTransportConfig(Map<String, Object> map)
      throws JsonProcessingException {
    // Convert the Map to a JSON string
    ObjectMapper objectMapper = new ObjectMapper();
    String jsonString = objectMapper.writeValueAsString(map);

    // Deserialize the JSON string to TransportConfig
    return objectMapper.readValue(jsonString, TransportConfig.class);
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
}
