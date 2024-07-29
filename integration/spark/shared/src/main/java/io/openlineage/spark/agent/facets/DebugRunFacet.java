/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.micrometer.core.instrument.Tag;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Value;

/**
 * Debug facet, that can be enabled on demand, to include as much interesting insights on
 * OpenLineage configuration and runtime as possible. Should be used to debug OpenLineage Spark
 * integration and provide details on potential problems.
 */
@Getter
public class DebugRunFacet extends OpenLineage.DefaultRunFacet {
  private final ClasspathDebugFacet classpath;
  private final SystemDebugFacet system;
  private final SparkConfigDebugFacet config;
  private final LogicalPlanDebugFacet logicalPlan;
  private final MetricsDebugFacet metrics;

  public DebugRunFacet(
      SparkConfigDebugFacet config,
      ClasspathDebugFacet classpath,
      SystemDebugFacet system,
      LogicalPlanDebugFacet logicalPlan,
      MetricsDebugFacet metricsDebugFacet) {
    super(Versions.OPEN_LINEAGE_PRODUCER_URI);
    this.config = config;
    this.classpath = classpath;
    this.system = system;
    this.logicalPlan = logicalPlan;
    this.metrics = metricsDebugFacet;
  }

  /** Entries from SparkConf that can be valuable for debugging. */
  @Value
  @Builder
  @AllArgsConstructor
  @JsonDeserialize(builder = SparkConfigDebugFacet.SparkConfigDebugFacetBuilder.class)
  public static class SparkConfigDebugFacet {
    // A comma-separated list of classes that implement SparkListener
    String extraListeners;
    // spark.openlineage. prefixed entries in Spark Conf with auth entries skipped
    SparkOpenLineageConfig openLineageConfig;
    // spark.sql.catalog.spark_catalog
    String catalogClass;
    // A comma-separated list of extensions classes
    String extensions;
  }

  @Value
  @Builder
  public static class ClasspathDebugFacet {
    String openLineageVersion;
    String sparkVersion;
    String scalaVersion;
    List<String> jars;
    List<ClasspathDebugClassDetails> classDetails;
  }

  @Value
  @Builder
  public static class ClasspathDebugClassDetails {
    String className;
    boolean isOnClasspath;
    String packageVersion;
  }

  /**
   * Information extracted from
   *
   * @see <a
   *     href="https://commons.apache.org/proper/commons-lang/apidocs/org/apache/commons/lang3/SystemUtils.html">SystemUtils</a>
   *     and from the
   * @see <a
   *     href="https://spark.apache.org/docs/3.5.0/api/java/org/apache/spark/sql/SparkSession.html">SparkSession</a>.
   */
  @Value
  @Builder
  public static class SystemDebugFacet {
    @Deprecated
    /**
     * @deprecated
     *     <p>Since version 1.15.0.
     *     <p>Will be removed in version 1.18.0.
     *     <p>Please use {@link SparkApplicationDetailsFacet} instead
     */
    String sparkDeployMode;

    String javaVersion;
    String javaVendor;
    String osArch;
    String osName;
    String osVersion;
    String userLanguage;
    String userTimezone;
  }

  /**
   * Should contain a LogicalPlan that contains only canonical names of the classes and their
   * hierarchy. We want to keep its content limited, in contrast to logicalPlan facet, to make sure
   * this does not cause serialization issues like cycled references or excessive payload size.
   */
  @Value
  @Getter
  public static class LogicalPlanDebugFacet {
    List<LogicalPlanNode> nodes;
  }

  @Value
  @Builder
  public static class LogicalPlanNode {
    String id;
    String desc;
    List<String> children;
  }

  /**
   * If enabled, DebugFacet will contain list of metric names, values and associated tags dumped at
   * the point of emitting the event which DebugFacet is contained by. The metrics are generated
   * using MeterRegistry micrometer mechanism, and are usually emitted via configured backend.
   */
  @Value
  @Getter
  public static class MetricsDebugFacet {
    List<MetricsNode> metrics;
  }

  /** Each {@link MetricsNode} object holds a specific metric's name, value and associated tags. */
  @Value
  @Builder
  public static class MetricsNode {
    /** The name of the metric represented by this node. */
    String name;

    /** The value of the metric represented by this node. */
    double value;

    /** A list of {@link Tag} associated with this metric node. */
    List<Tag> tags;
  }
}
