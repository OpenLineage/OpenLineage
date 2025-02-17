/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.ImmutableList;
import io.openlineage.client.MergeConfig;
import io.openlineage.client.OpenLineageConfig;
import io.openlineage.client.circuitBreaker.CircuitBreakerConfig;
import io.openlineage.client.dataset.DatasetConfig;
import io.openlineage.client.transports.FacetsConfig;
import io.openlineage.client.transports.TransportConfig;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

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
  private VendorsConfig vendors;
  private RunConfig run;

  @JsonProperty("columnLineage")
  private ColumnLineageConfig columnLineageConfig;

  @JsonProperty("filter")
  private FilterConfig filterConfig;

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
      ColumnLineageConfig columnLineageConfig,
      VendorsConfig vendors,
      FilterConfig filterConfig,
      RunConfig run) {
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
    this.vendors = vendors;
    this.filterConfig = filterConfig;
    this.run = run;
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
  public static class JobConfig implements MergeConfig<JobConfig> {
    private JobOwnersConfig owners;

    @JsonDeserialize(using = TagsDeserializer.class)
    private List<TagField> tags;

    @Override
    public JobConfig mergeWithNonNull(JobConfig other) {
      Map<String, TagField> tagMap =
          tags.stream().collect(Collectors.toMap(TagField::getKey, x -> x));
      other.getTags().forEach(tag -> tagMap.put(tag.getKey(), tag));
      JobConfig jobConfig = new JobConfig();

      JobOwnersConfig newOwners = new JobOwnersConfig();
      newOwners.getAdditionalProperties().putAll(owners.getAdditionalProperties());
      newOwners.getAdditionalProperties().putAll(other.owners.getAdditionalProperties());
      jobConfig.setOwners(newOwners);
      jobConfig.setTags(new ArrayList<>(tagMap.values()));
      return jobConfig;
    }
  }

  @Getter
  @ToString
  public static class JobOwnersConfig {
    @JsonAnySetter @NonNull
    private final Map<String, String> additionalProperties = new HashMap<>();
  }

  @Getter
  @Setter
  @ToString
  public static class RunConfig implements MergeConfig<RunConfig> {
    @JsonDeserialize(using = TagsDeserializer.class)
    private List<TagField> tags;

    @Override
    public RunConfig mergeWithNonNull(RunConfig other) {
      Map<String, TagField> tagMap =
          tags.stream().collect(Collectors.toMap(TagField::getKey, x -> x));
      other.getTags().forEach(tag -> tagMap.put(tag.getKey(), tag));
      RunConfig runConfig = new RunConfig();
      runConfig.setTags(new ArrayList<>(tagMap.values()));
      return runConfig;
    }
  }

  @Getter
  @Setter
  @ToString
  public static class VendorsConfig {
    @JsonAnySetter @NonNull
    private final Map<String, String> additionalProperties = new HashMap<>();
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
        mergePropertyWith(overriddenAppName, other.overriddenAppName),
        mergePropertyWith(testExtensionProvider, other.testExtensionProvider),
        mergePropertyWith(jobName, other.jobName),
        mergePropertyWith(job, other.job),
        mergePropertyWith(transportConfig, other.transportConfig),
        mergePropertyWith(facetsConfig, other.facetsConfig),
        mergePropertyWith(datasetConfig, other.datasetConfig),
        mergePropertyWith(circuitBreaker, other.circuitBreaker),
        mergePropertyWith(metricsConfig, other.metricsConfig),
        mergePropertyWith(columnLineageConfig, other.columnLineageConfig),
        mergePropertyWith(vendors, other.vendors),
        mergePropertyWith(filterConfig, other.filterConfig),
        mergePropertyWith(run, other.run));
  }

  public static class TagsDeserializer extends JsonDeserializer<List<TagField>> {
    @Override
    public List<TagField> deserialize(
        JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
      JsonNode node = jsonParser.getCodec().readTree(jsonParser);
      Stream<String> tagText;
      if (node.isArray()) {
        tagText = StreamSupport.stream(node.spliterator(), false).map(JsonNode::asText);
      } else if (node.isTextual()) {
        tagText = Arrays.stream(node.asText().split(";"));
      } else {
        return Collections.emptyList();
      }
      return tagText
          .filter(StringUtils::isNotBlank)
          .flatMap(
              x -> {
                String[] elements = x.split(":");
                String key, value = "true", source = "CONFIG";
                if (elements.length == 0) {
                  return Stream.empty();
                }

                // If we get malformed data, we skip the tag.
                if (StringUtils.isBlank(elements[0])) {
                  return Stream.empty();
                }
                key = elements[0];
                if (elements.length >= 2) {
                  // If we get malformed data, we skip the tag.
                  if (StringUtils.isBlank(elements[1])) {
                    return Stream.empty();
                  }
                  value = elements[1];
                }
                if (elements.length >= 3) {
                  // If elements.length > 3, truncate the rest of the elements - use only
                  // first three ones.
                  // If we get malformed data, we skip the tag.
                  if (StringUtils.isBlank(elements[2])) {
                    return Stream.empty();
                  }
                  source = elements[2];
                }
                return Stream.of(new TagField(key, value, source));
              })
          .collect(Collectors.toList());
    }
  }
}
