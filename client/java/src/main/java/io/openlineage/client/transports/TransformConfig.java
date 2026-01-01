/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.client.MergeConfig;
import java.util.Map;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * CompositeConfig is a configuration class for CompositeTransport, implementing TransportConfig.
 */
@ToString
@NoArgsConstructor
public final class TransformConfig implements TransportConfig, MergeConfig<TransformConfig> {

  @Getter @Setter private TransportConfig transport;

  @Getter @Setter private String transformerClass;

  @Getter @Setter private Map<String, String> transformerProperties;

  @JsonCreator
  @SuppressWarnings("unchecked")
  public TransformConfig(
      @JsonProperty("transport") Object transport,
      @JsonProperty("transformClass") String transformClass) {
    this.transformerClass = transformClass;
    this.transport = createTransportConfig((Map<String, Object>) transport);
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
  public TransformConfig mergeWithNonNull(TransformConfig other) {
    // Merge the transports and continueOnFailure fields from both configs
    TransportConfig mergedTransport = mergePropertyWith(transport, other.getTransport());
    String mergedTransformClass = mergePropertyWith(transformerClass, other.transformerClass);

    return new TransformConfig(mergedTransport, mergedTransformClass);
  }
}
