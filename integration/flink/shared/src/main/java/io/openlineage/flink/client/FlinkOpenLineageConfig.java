/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.client;

import io.openlineage.client.OpenLineageConfig;
import io.openlineage.client.circuitBreaker.CircuitBreakerConfig;
import io.openlineage.client.transports.FacetsConfig;
import io.openlineage.client.transports.TransportConfig;
import java.util.Map;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Getter
@NoArgsConstructor
@ToString
public class FlinkOpenLineageConfig extends OpenLineageConfig<FlinkOpenLineageConfig> {

  public FlinkOpenLineageConfig(
      TransportConfig transportConfig,
      FacetsConfig facetsConfig,
      CircuitBreakerConfig circuitBreaker,
      Map metricsConfig) {
    super(transportConfig, facetsConfig, circuitBreaker, metricsConfig);
  }

  public FlinkOpenLineageConfig mergeWithNonNull(FlinkOpenLineageConfig other) {
    return new FlinkOpenLineageConfig(
        mergePropertyWith(transportConfig, other.transportConfig),
        mergePropertyWith(facetsConfig, other.facetsConfig),
        mergePropertyWith(circuitBreaker, other.circuitBreaker),
        mergePropertyWith(metricsConfig, other.metricsConfig));
  }
}
