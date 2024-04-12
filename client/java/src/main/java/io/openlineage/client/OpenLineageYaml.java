/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.openlineage.client.circuitBreaker.CircuitBreakerConfig;
import io.openlineage.client.transports.FacetsConfig;
import io.openlineage.client.transports.TransportConfig;
import java.util.Map;
import lombok.Getter;

/** Configuration for {@link OpenLineageClient}. */
@JsonIgnoreProperties
@Getter
public class OpenLineageYaml {
  @JsonProperty("transport")
  private TransportConfig transportConfig;

  @JsonProperty("facets")
  private FacetsConfig facetsConfig;

  @JsonProperty("circuitBreaker")
  private CircuitBreakerConfig circuitBreaker;

  @JsonProperty("metrics")
  private Map<String, Object> metricsConfig;
}
