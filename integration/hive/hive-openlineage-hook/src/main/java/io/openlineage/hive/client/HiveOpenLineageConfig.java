/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.client;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import io.openlineage.client.OpenLineageConfig;
import io.openlineage.client.circuitBreaker.CircuitBreakerConfig;
import io.openlineage.client.dataset.DatasetConfig;
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
public class HiveOpenLineageConfig extends OpenLineageConfig<HiveOpenLineageConfig> {
  @Setter private JobConfig job;

  public HiveOpenLineageConfig(
      TransportConfig transportConfig,
      FacetsConfig facetsConfig,
      DatasetConfig datasetConfig,
      CircuitBreakerConfig circuitBreaker,
      Map metricsConfig,
      JobConfig job) {
    super(transportConfig, facetsConfig, datasetConfig, circuitBreaker, metricsConfig);
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

  @Override
  public HiveOpenLineageConfig mergeWithNonNull(HiveOpenLineageConfig other) {
    return new HiveOpenLineageConfig(
        mergePropertyWith(transportConfig, other.transportConfig),
        mergePropertyWith(facetsConfig, other.facetsConfig),
        mergePropertyWith(datasetConfig, other.datasetConfig),
        mergePropertyWith(circuitBreaker, other.circuitBreaker),
        mergePropertyWith(metricsConfig, other.metricsConfig),
        mergePropertyWith(job, other.job));
  }
}
