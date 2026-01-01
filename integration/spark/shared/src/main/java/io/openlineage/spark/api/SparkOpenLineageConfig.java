/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.openlineage.client.OpenLineageConfig;
import io.openlineage.client.circuitBreaker.CircuitBreakerConfig;
import io.openlineage.client.dataset.DatasetConfig;
import io.openlineage.client.job.JobConfig;
import io.openlineage.client.run.RunConfig;
import io.openlineage.client.transports.FacetsConfig;
import io.openlineage.client.transports.TransportConfig;
import java.util.*;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Data;
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
  private String rootParentJobName;
  private String rootParentJobNamespace;
  private String rootParentRunId;
  private String overriddenAppName;
  private String overriddenApplicationRunId;
  private String testExtensionProvider;
  private JobNameConfig jobName;

  @JsonProperty("vendors")
  private VendorsConfig vendors;

  @JsonProperty("columnLineage")
  private ColumnLineageConfig columnLineageConfig;

  @JsonProperty("filter")
  private FilterConfig filterConfig;

  @JsonProperty("debug")
  private DebugConfig debugConfig;

  @JsonProperty("timeout")
  private TimeoutConfig timeoutConfig;

  public SparkOpenLineageConfig(
      String namespace,
      String parentJobName,
      String parentJobNamespace,
      String parentRunId,
      String rootParentJobName,
      String rootParentJobNamespace,
      String rootParentRunId,
      String overriddenAppName,
      String overriddenApplicationRunId,
      String testExtensionProvider,
      JobNameConfig jobName,
      JobConfig job,
      TransportConfig transportConfig,
      FacetsConfig facetsConfig,
      DatasetConfig datasetConfig,
      CircuitBreakerConfig circuitBreaker,
      Map<String, Object> metricsConfig,
      ColumnLineageConfig columnLineageConfig,
      VendorsConfig vendors,
      FilterConfig filterConfig,
      RunConfig run) {
    super(transportConfig, facetsConfig, datasetConfig, circuitBreaker, metricsConfig, run, job);
    this.namespace = namespace;
    this.parentJobName = parentJobName;
    this.parentJobNamespace = parentJobNamespace;
    this.parentRunId = parentRunId;
    this.rootParentJobName = rootParentJobName;
    this.rootParentJobNamespace = rootParentJobNamespace;
    this.rootParentRunId = rootParentRunId;
    this.overriddenAppName = overriddenAppName;
    this.overriddenApplicationRunId = overriddenApplicationRunId;
    this.testExtensionProvider = testExtensionProvider;
    this.jobName = jobName;
    this.columnLineageConfig = columnLineageConfig;
    this.vendors = vendors;
    this.filterConfig = filterConfig;
  }

  @Override
  public FacetsConfig getFacetsConfig() {
    if (facetsConfig == null) {
      facetsConfig = new FacetsConfig();
    }

    Map<String, Boolean> DISABLED_BY_DEFAULT_MAP =
        DISABLED_BY_DEFAULT.stream().collect(Collectors.toMap(facet -> facet, facet -> true));
    facetsConfig.setDisabledFacets(
        mergePropertyWith(DISABLED_BY_DEFAULT_MAP, facetsConfig.getDisabledFacets()));

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
      columnLineageConfig.setSchemaSizeLimit(1_000);
      columnLineageConfig.setDatasetLineageEnabled(true);
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
  public static class VendorsConfig {
    @JsonAnySetter @NonNull private final Map<String, VendorConfig> config = new HashMap<>();

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class VendorConfig {
      @NonNull private Boolean metricsReporterDisabled = false;
    }
  }

  @Getter
  @Setter
  @ToString
  public static class FilterConfig {
    private final List<String> allowedSparkNodes = new ArrayList<>();
    private final List<String> deniedSparkNodes = new ArrayList<>();
  }

  @Override
  public SparkOpenLineageConfig mergeWithNonNull(SparkOpenLineageConfig other) {
    return new SparkOpenLineageConfig(
        mergeWithDefaultValue(namespace, other.namespace, DEFAULT_NAMESPACE),
        mergePropertyWith(parentJobName, other.parentJobName),
        mergePropertyWith(parentJobNamespace, other.parentJobNamespace),
        mergePropertyWith(parentRunId, other.parentRunId),
        mergePropertyWith(rootParentJobName, other.rootParentJobName),
        mergePropertyWith(rootParentJobNamespace, other.rootParentJobNamespace),
        mergePropertyWith(rootParentRunId, other.rootParentRunId),
        mergePropertyWith(overriddenAppName, other.overriddenAppName),
        mergePropertyWith(overriddenApplicationRunId, other.overriddenApplicationRunId),
        mergePropertyWith(testExtensionProvider, other.testExtensionProvider),
        mergePropertyWith(jobName, other.jobName),
        mergePropertyWith(jobConfig, other.jobConfig),
        mergePropertyWith(transportConfig, other.transportConfig),
        mergePropertyWith(facetsConfig, other.facetsConfig),
        mergePropertyWith(datasetConfig, other.datasetConfig),
        mergePropertyWith(circuitBreaker, other.circuitBreaker),
        mergePropertyWith(metricsConfig, other.metricsConfig),
        mergePropertyWith(columnLineageConfig, other.columnLineageConfig),
        mergePropertyWith(vendors, other.vendors),
        mergePropertyWith(filterConfig, other.filterConfig),
        mergePropertyWith(runConfig, other.runConfig));
  }
}
