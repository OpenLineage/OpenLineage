/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

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

/** Config class to store entries which are specific only to Spark integration. */
@Getter
@NoArgsConstructor
@ToString
public class SparkOpenLineageConfig extends OpenLineageConfig<SparkOpenLineageConfig> {

  public static final String DEFAULT_NAMESPACE = "default";
  public static final String[] DEFAULT_DISABLED_FACETS =
      new String[] {"spark_unknown", "spark.logicalPlan"};
  public static final String DEFAULT_DEBUG_FACET = "disabled";

  @Setter @NonNull private String namespace;
  @Setter @Getter private String parentJobName;
  @Setter @Getter private String parentJobNamespace;
  @Setter @Getter private String parentRunId;
  @Setter @Getter private String overriddenAppName;
  @Setter @NonNull private String debugFacet;
  @Setter @Getter private String testExtensionProvider;
  @Setter private JobNameConfig jobName;
  @Setter private JobConfig job;

  public SparkOpenLineageConfig(
      String namespace,
      String parentJobName,
      String parentJobNamespace,
      String parentRunId,
      String overriddenAppName,
      String debugFacet,
      String testExtensionProvider,
      JobNameConfig jobName,
      JobConfig job,
      TransportConfig transportConfig,
      FacetsConfig facetsConfig,
      DatasetConfig datasetConfig,
      CircuitBreakerConfig circuitBreaker,
      Map<String, Object> metricsConfig) {
    super(transportConfig, facetsConfig, datasetConfig, circuitBreaker, metricsConfig);
    this.namespace = namespace;
    this.parentJobName = parentJobName;
    this.parentJobNamespace = parentJobNamespace;
    this.parentRunId = parentRunId;
    this.overriddenAppName = overriddenAppName;
    this.debugFacet = debugFacet;
    this.testExtensionProvider = testExtensionProvider;
    this.jobName = jobName;
    this.job = job;
  }

  @Override
  public FacetsConfig getFacetsConfig() {
    if (facetsConfig == null) {
      facetsConfig = new FacetsConfig();
    }
    if (facetsConfig.getDisabledFacets() == null) {
      facetsConfig.setDisabledFacets(DEFAULT_DISABLED_FACETS);
    }
    return facetsConfig;
  }

  public JobNameConfig getJobName() {
    if (jobName == null) {
      jobName = new JobNameConfig();
    }
    return jobName;
  }

  public String getNamespace() {
    if (namespace == null) {
      namespace = DEFAULT_NAMESPACE;
    }
    return namespace;
  }

  public String getDebugFacet() {
    if (debugFacet == null) {
      debugFacet = DEFAULT_DEBUG_FACET;
    }
    return debugFacet;
  }

  @Getter
  @ToString
  public static class JobNameConfig {
    @Setter @Getter @NonNull private Boolean appendDatasetName = true;
    @Setter @Getter @NonNull private Boolean replaceDotWithUnderscore = false;
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
  public SparkOpenLineageConfig mergeWithNonNull(SparkOpenLineageConfig other) {
    return new SparkOpenLineageConfig(
        mergeWithDefaultValue(namespace, other.namespace, DEFAULT_NAMESPACE),
        mergePropertyWith(parentJobName, other.parentJobName),
        mergePropertyWith(parentJobNamespace, other.parentJobNamespace),
        mergePropertyWith(parentRunId, other.parentRunId),
        mergePropertyWith(overriddenAppName, other.overriddenAppName),
        mergeWithDefaultValue(debugFacet, other.debugFacet, DEFAULT_DEBUG_FACET),
        mergePropertyWith(testExtensionProvider, other.testExtensionProvider),
        mergePropertyWith(jobName, other.jobName),
        mergePropertyWith(job, other.job),
        mergePropertyWith(transportConfig, other.transportConfig),
        mergePropertyWith(facetsConfig, other.facetsConfig),
        mergePropertyWith(datasetConfig, other.datasetConfig),
        mergePropertyWith(circuitBreaker, other.circuitBreaker),
        mergePropertyWith(metricsConfig, other.metricsConfig));
  }
}
