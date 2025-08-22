/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.openlineage.client.circuitBreaker.CircuitBreakerConfig;
import io.openlineage.client.dataset.DatasetConfig;
import io.openlineage.client.job.JobConfig;
import io.openlineage.client.run.RunConfig;
import io.openlineage.client.transports.FacetsConfig;
import io.openlineage.client.transports.TransportConfig;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * Configuration for {@link OpenLineageClient}.
 *
 * @param <T> generic type extending {@link OpenLineageConfig}, used for merging config objects
 */
@JsonIgnoreProperties
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class OpenLineageConfig<T extends OpenLineageConfig> implements MergeConfig<T> {
  @JsonProperty("transport")
  protected TransportConfig transportConfig;

  @JsonProperty("facets")
  protected FacetsConfig facetsConfig;

  @JsonProperty("dataset")
  protected DatasetConfig datasetConfig;

  @JsonProperty("circuitBreaker")
  protected CircuitBreakerConfig circuitBreaker;

  @JsonProperty("metrics")
  protected Map<String, Object> metricsConfig;

  @JsonProperty("run")
  protected RunConfig runConfig;

  @JsonProperty("job")
  protected JobConfig jobConfig;

  /**
   * Overwrites existing object with properties of other config entries whenever they're present.
   *
   * @param other value to merge
   * @return merged config entry
   */
  @Override
  public OpenLineageConfig mergeWithNonNull(OpenLineageConfig other) {
    return new OpenLineageConfig(
        mergePropertyWith(transportConfig, other.transportConfig),
        mergePropertyWith(facetsConfig, other.facetsConfig),
        mergePropertyWith(datasetConfig, other.datasetConfig),
        mergePropertyWith(circuitBreaker, other.circuitBreaker),
        mergePropertyWith(metricsConfig, other.metricsConfig),
        mergePropertyWith(runConfig, other.runConfig),
        mergePropertyWith(jobConfig, other.jobConfig));
  }
}
