/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.client;

import io.openlineage.client.OpenLineageConfig;
import io.openlineage.client.circuitBreaker.CircuitBreakerConfig;
import io.openlineage.client.dataset.DatasetConfig;
import io.openlineage.client.job.JobConfig;
import io.openlineage.client.run.RunConfig;
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
      DatasetConfig datasetConfig,
      CircuitBreakerConfig circuitBreaker,
      Map metricsConfig,
      RunConfig runConfig,
      JobConfig jobConfig) {
    super(
        transportConfig,
        facetsConfig,
        datasetConfig,
        circuitBreaker,
        metricsConfig,
        runConfig,
        jobConfig);
  }

  @Override
  public FlinkOpenLineageConfig mergeWithNonNull(FlinkOpenLineageConfig other) {
    return new FlinkOpenLineageConfig(
        mergePropertyWith(transportConfig, other.transportConfig),
        mergePropertyWith(facetsConfig, other.facetsConfig),
        mergePropertyWith(datasetConfig, other.datasetConfig),
        mergePropertyWith(circuitBreaker, other.circuitBreaker),
        mergePropertyWith(metricsConfig, other.metricsConfig),
        mergePropertyWith(runConfig, other.runConfig),
        mergePropertyWith(jobConfig, other.jobConfig));
  }
}
