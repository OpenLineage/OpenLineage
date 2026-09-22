/*
/* Copyright 2018-2026 contributors to the OpenLineage project
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

  @JsonProperty("lineage")
  protected LineageConfig lineageConfig;

  /** Preserves the existing constructor used by integration-specific configuration classes. */
  public OpenLineageConfig(
      TransportConfig transportConfig,
      FacetsConfig facetsConfig,
      DatasetConfig datasetConfig,
      CircuitBreakerConfig circuitBreaker,
      Map<String, Object> metricsConfig,
      RunConfig runConfig,
      JobConfig jobConfig) {
    this(
        transportConfig,
        facetsConfig,
        datasetConfig,
        circuitBreaker,
        metricsConfig,
        runConfig,
        jobConfig,
        null);
  }

  public OpenLineageConfig(
      TransportConfig transportConfig,
      FacetsConfig facetsConfig,
      DatasetConfig datasetConfig,
      CircuitBreakerConfig circuitBreaker,
      Map<String, Object> metricsConfig,
      RunConfig runConfig,
      JobConfig jobConfig,
      LineageConfig lineageConfig) {
    this.transportConfig = transportConfig;
    this.facetsConfig = facetsConfig;
    this.datasetConfig = datasetConfig;
    this.circuitBreaker = circuitBreaker;
    this.metricsConfig = metricsConfig;
    this.runConfig = runConfig;
    this.jobConfig = jobConfig;
    this.lineageConfig = lineageConfig;
  }

  public LineageConfig getLineageConfig() {
    if (lineageConfig == null) {
      lineageConfig = new LineageConfig();
    }
    return lineageConfig;
  }

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
        mergePropertyWith(jobConfig, other.jobConfig),
        mergePropertyWith(lineageConfig, other.lineageConfig));
  }
}
