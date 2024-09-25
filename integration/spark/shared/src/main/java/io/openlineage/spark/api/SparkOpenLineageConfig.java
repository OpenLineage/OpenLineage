/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.openlineage.client.OpenLineageConfig;
import io.openlineage.client.circuitBreaker.CircuitBreakerConfig;
import io.openlineage.client.dataset.DatasetConfig;
import io.openlineage.client.transports.FacetsConfig;
import io.openlineage.client.transports.TransportConfig;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

/** Config class to store entries which are specific only to Spark integration. */
@Getter
@Setter
@NoArgsConstructor
@ToString
public class SparkOpenLineageConfig extends OpenLineageConfig<SparkOpenLineageConfig> {
  public static List<String> DISABLED_BY_DEFAULT =
      ImmutableList.of("spark_unknown", "spark.logicalPlan", "debug");

  public static final String DEFAULT_NAMESPACE = "default";

  @NonNull private String namespace;
  private String parentJobName;
  private String parentJobNamespace;
  private String parentRunId;
  private String overriddenAppName;
  @NonNull private String debugFacet;
  private String testExtensionProvider;
  private JobNameConfig jobName;
  private JobConfig job;

  @JsonProperty("columnLineage")
  private ColumnLineageConfig columnLineageConfig;

  public SparkOpenLineageConfig(
      String namespace,
      String parentJobName,
      String parentJobNamespace,
      String parentRunId,
      String overriddenAppName,
      String testExtensionProvider,
      JobNameConfig jobName,
      JobConfig job,
      TransportConfig transportConfig,
      FacetsConfig facetsConfig,
      DatasetConfig datasetConfig,
      CircuitBreakerConfig circuitBreaker,
      Map<String, Object> metricsConfig,
      ColumnLineageConfig columnLineageConfig) {
    super(transportConfig, facetsConfig, datasetConfig, circuitBreaker, metricsConfig);
    this.namespace = namespace;
    this.parentJobName = parentJobName;
    this.parentJobNamespace = parentJobNamespace;
    this.parentRunId = parentRunId;
    this.overriddenAppName = overriddenAppName;
    this.testExtensionProvider = testExtensionProvider;
    this.jobName = jobName;
    this.job = job;
    this.columnLineageConfig = columnLineageConfig;
  }

  @Override
  public FacetsConfig getFacetsConfig() {
    if (facetsConfig == null) {
      facetsConfig = new FacetsConfig();
    }
    if (facetsConfig.getDeprecatedDisabledFacets() == null) {
      facetsConfig.setDeprecatedDisabledFacets(new String[] {});
    }
    if (facetsConfig.getDisabledFacets() == null) {
      facetsConfig.setDisabledFacets(new HashMap<>());
    } else {
      Map<String, Boolean> DISABLED_BY_DEFAULT_MAP =
          DISABLED_BY_DEFAULT.stream().collect(Collectors.toMap(facet -> facet, facet -> true));
      facetsConfig.setDisabledFacets(
          mergePropertyWith(DISABLED_BY_DEFAULT_MAP, facetsConfig.getDisabledFacets()));
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

  public ColumnLineageConfig getColumnLineageConfig() {
    if (columnLineageConfig == null) {
      columnLineageConfig = new ColumnLineageConfig();
      // TODO #3084: For the release 1.26.0 this flag should default to true
      columnLineageConfig.setDatasetLineageEnabled(false);
    }
    return columnLineageConfig;
  }

  @Getter
  @Setter
  @ToString
  public static class JobNameConfig {
    @NonNull private Boolean appendDatasetName = true;
    @NonNull private Boolean replaceDotWithUnderscore = false;
  }

  @Getter
  @Setter
  @ToString
  public static class JobConfig {
    private JobOwnersConfig owners;
  }

  @Getter
  @ToString
  public static class JobOwnersConfig {
    @JsonAnySetter @NonNull
    private final Map<String, String> additionalProperties = new HashMap<>();
  }

  @Override
  public SparkOpenLineageConfig mergeWithNonNull(SparkOpenLineageConfig other) {
    return new SparkOpenLineageConfig(
        mergeWithDefaultValue(namespace, other.namespace, DEFAULT_NAMESPACE),
        mergePropertyWith(parentJobName, other.parentJobName),
        mergePropertyWith(parentJobNamespace, other.parentJobNamespace),
        mergePropertyWith(parentRunId, other.parentRunId),
        mergePropertyWith(overriddenAppName, other.overriddenAppName),
        mergePropertyWith(testExtensionProvider, other.testExtensionProvider),
        mergePropertyWith(jobName, other.jobName),
        mergePropertyWith(job, other.job),
        mergePropertyWith(transportConfig, other.transportConfig),
        mergePropertyWith(facetsConfig, other.facetsConfig),
        mergePropertyWith(datasetConfig, other.datasetConfig),
        mergePropertyWith(circuitBreaker, other.circuitBreaker),
        mergePropertyWith(metricsConfig, other.metricsConfig),
        mergePropertyWith(columnLineageConfig, other.columnLineageConfig));
  }
}
