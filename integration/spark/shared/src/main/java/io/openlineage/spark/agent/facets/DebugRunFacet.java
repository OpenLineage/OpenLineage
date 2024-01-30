/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import java.util.List;
import java.util.Map;
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

  public DebugRunFacet(
      SparkConfigDebugFacet config,
      ClasspathDebugFacet classpath,
      SystemDebugFacet system,
      LogicalPlanDebugFacet logicalPlan) {
    super(Versions.OPEN_LINEAGE_PRODUCER_URI);
    this.config = config;
    this.classpath = classpath;
    this.system = system;
    this.logicalPlan = logicalPlan;
  }

  /** Entries from SparkConf that can be valuable for debugging. */
  @Value
  @Builder
  public static class SparkConfigDebugFacet {
    // A comma-separated list of classes that implement SparkListener
    String extraListeners;
    // spark.openlineage. prefixed entries in Spark Conf with auth entries skipped
    Map<String, String> openLineageConfig;
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
   * Information extracted from {@link
   * https://commons.apache.org/proper/commons-lang/apidocs/org/apache/commons/lang3/SystemUtils.html}(SystemUtils)
   * and from the {@link
   * https://spark.apache.org/docs/3.5.0/api/java/org/apache/spark/sql/SparkSession.html}(SparkSession).
   */
  @Value
  @Builder
  public static class SystemDebugFacet {
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
}
