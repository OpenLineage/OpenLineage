/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.client.MergeConfig;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * CompositeConfig is a configuration class for CompositeTransport, implementing TransportConfig.
 */
@ToString
@NoArgsConstructor
public final class CompositeConfig implements TransportConfig, MergeConfig<CompositeConfig> {

  @Getter @Setter private List<TransportConfig> transports;

  @Getter @Setter private boolean continueOnFailure;

  @JsonCreator
  @SuppressWarnings("unchecked")
  public CompositeConfig(
      @JsonProperty("transports") Object transports,
      @JsonProperty("continueOnFailure") boolean continueOnFailure) {

    if (transports instanceof List) {
      // Handle List<Map<String, Object>> case
      this.transports =
          ((List<Map<String, Object>>) transports)
              .stream().map(this::createTransportConfig).collect(Collectors.toList());
    } else if (transports instanceof Map) {
      // Handle Map<String, Object> case
      Map<String, Object> transportMap = (Map<String, Object>) transports;
      this.transports =
          transportMap.entrySet().stream()
              .map(
                  entry -> {
                    Map<String, Object> nestedMap = new HashMap<>();
                    nestedMap.putAll((Map<String, Object>) entry.getValue());
                    nestedMap.put("name", entry.getKey());
                    return nestedMap;
                  })
              .map(this::createTransportConfig)
              .collect(Collectors.toList());
    } else {
      throw new IllegalArgumentException("Invalid transports type");
    }

    this.continueOnFailure = continueOnFailure;
  }

  public static CompositeConfig createFromTransportConfigs(
      List<TransportConfig> transports, boolean continueOnFailure) {
    CompositeConfig compositeConfig = new CompositeConfig();
    compositeConfig.setTransports(transports);
    compositeConfig.setContinueOnFailure(continueOnFailure);
    return compositeConfig;
  }

  private TransportConfig createTransportConfig(Map<String, Object> map) {
    // Convert the Map to a JSON string
    ObjectMapper objectMapper = new ObjectMapper();
    String jsonString;
    try {
      jsonString = objectMapper.writeValueAsString(map);
      return objectMapper.readValue(jsonString, TransportConfig.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Error creating transport config", e);
    }
  }

  @Override
  public CompositeConfig mergeWithNonNull(CompositeConfig other) {
    // Merge the transports and continueOnFailure fields from both configs
    List<TransportConfig> mergedTransports = mergePropertyWith(transports, other.getTransports());
    boolean mergedContinueOnFailure =
        mergePropertyWith(continueOnFailure, other.isContinueOnFailure());

    return createFromTransportConfigs(mergedTransports, mergedContinueOnFailure);
  }
}
