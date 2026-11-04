/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.openlineage.client.OpenLineageConfig;
import io.openlineage.client.circuitBreaker.CircuitBreakerConfig;
import io.openlineage.client.job.JobConfig;
import io.openlineage.client.run.RunConfig;
import io.openlineage.client.transports.FacetsConfig;
import io.openlineage.client.transports.TransportConfig;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@ToString(callSuper = true)
public class FlinkOpenLineageConfig extends OpenLineageConfig<FlinkOpenLineageConfig> {

  private static final Integer DEFAULT_TRACKING_INTERVAL = 60;
  private static final Integer DEFAULT_DETACHED_START_EVENT_EMIT_TIMEOUT = 5;
  private static final Boolean DEFAULT_ENABLE_DETACHED_JOB_TRACKING = false;

  @JsonProperty("dataset")
  @Setter
  @Getter
  private FlinkDatasetConfig datasetConfig;

  @JsonProperty("trackingIntervalInSeconds")
  @Setter
  private Integer trackingIntervalInSeconds;

  @JsonProperty("detachedStartEventEmitTimeoutInSeconds")
  @Setter
  private Integer detachedStartEventEmitTimeoutInSeconds;

  @JsonProperty("enableDetachedJobTracking")
  @Setter
  private Boolean enableDetachedJobTracking;

  @JsonProperty("restApiBaseUrl")
  @Setter
  @Getter
  private String restApiBaseUrl;

  @JsonProperty("disableCheckpointTracking")
  @Setter
  private Boolean disableCheckpointTracking;

  public FlinkOpenLineageConfig() {
    super();
    datasetConfig = new FlinkDatasetConfig();
  }

  public FlinkOpenLineageConfig(
      TransportConfig transportConfig,
      FacetsConfig facetsConfig,
      FlinkDatasetConfig datasetConfig,
      CircuitBreakerConfig circuitBreaker,
      Map metricsConfig,
      RunConfig runConfig,
      JobConfig jobConfig,
      Integer trackingIntervalInSeconds,
      Integer detachedStartEventEmitTimeoutInSeconds,
      Boolean enableDetachedJobTracking,
      String restApiBaseUrl,
      Boolean disableCheckpointTracking) {
    super(
        transportConfig,
        facetsConfig,
        datasetConfig,
        circuitBreaker,
        metricsConfig,
        runConfig,
        jobConfig);
    this.datasetConfig = datasetConfig;
    this.trackingIntervalInSeconds = trackingIntervalInSeconds;
    this.detachedStartEventEmitTimeoutInSeconds = detachedStartEventEmitTimeoutInSeconds;
    this.enableDetachedJobTracking = enableDetachedJobTracking;
    this.restApiBaseUrl = restApiBaseUrl;
    this.disableCheckpointTracking = disableCheckpointTracking;
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
        mergePropertyWith(jobConfig, other.jobConfig),
        mergePropertyWith(trackingIntervalInSeconds, other.trackingIntervalInSeconds),
        mergePropertyWith(
            detachedStartEventEmitTimeoutInSeconds, other.detachedStartEventEmitTimeoutInSeconds),
        mergePropertyWith(enableDetachedJobTracking, other.enableDetachedJobTracking),
        mergePropertyWith(restApiBaseUrl, other.restApiBaseUrl),
        mergePropertyWith(disableCheckpointTracking, other.disableCheckpointTracking));
  }

  public Integer getTrackingIntervalInSeconds() {
    return Optional.ofNullable(trackingIntervalInSeconds).orElse(DEFAULT_TRACKING_INTERVAL);
  }

  public Integer getDetachedStartEventEmitTimeoutInSeconds() {
    return Optional.ofNullable(detachedStartEventEmitTimeoutInSeconds)
        .orElse(DEFAULT_DETACHED_START_EVENT_EMIT_TIMEOUT);
  }

  public Boolean getEnableDetachedJobTracking() {
    return Optional.ofNullable(enableDetachedJobTracking)
        .orElse(DEFAULT_ENABLE_DETACHED_JOB_TRACKING);
  }

  public Boolean getDisableCheckpointTracking() {
    return Optional.ofNullable(disableCheckpointTracking).orElse(false);
  }
}
