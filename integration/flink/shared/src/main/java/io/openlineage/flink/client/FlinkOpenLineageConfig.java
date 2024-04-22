/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.client;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import io.openlineage.client.OpenLineageConfig;
import io.openlineage.client.circuitBreaker.CircuitBreakerConfig;
import io.openlineage.client.transports.FacetsConfig;
import io.openlineage.client.transports.TransportConfig;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

@Getter
@NoArgsConstructor
@ToString
public class FlinkOpenLineageConfig extends OpenLineageConfig<FlinkOpenLineageConfig> {
  @Setter private JobConfig job;

  public FlinkOpenLineageConfig(
      TransportConfig transportConfig,
      FacetsConfig facetsConfig,
      CircuitBreakerConfig circuitBreaker,
      Map metricsConfig,
      JobConfig job) {
    super(transportConfig, facetsConfig, circuitBreaker, metricsConfig);
    this.job = job;
  }

  @Getter
  @ToString
  public static class JobConfig {
    @Setter @Getter private JobOwnersConfig owners;
  }

  @Getter
  @ToString
  public static class JobOwnersConfig {
    @JsonAnySetter @Setter @Getter @NonNull
    private Map<String, String> additionalProperties = new HashMap<>();
  }

  public FlinkOpenLineageConfig mergeWithNonNull(FlinkOpenLineageConfig other) {
    return new FlinkOpenLineageConfig(
        mergePropertyWith(transportConfig, other.transportConfig),
        mergePropertyWith(facetsConfig, other.facetsConfig),
        mergePropertyWith(circuitBreaker, other.circuitBreaker),
        mergePropertyWith(metricsConfig, other.metricsConfig),
        mergePropertyWith(job, other.job));
  }
}
