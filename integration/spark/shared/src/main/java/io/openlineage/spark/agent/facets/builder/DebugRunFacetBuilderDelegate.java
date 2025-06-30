/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import io.openlineage.spark.agent.facets.DebugRunFacet;
import io.openlineage.spark.agent.facets.DebugRunFacet.ClasspathDebugClassDetails;
import io.openlineage.spark.agent.facets.DebugRunFacet.ClasspathDebugClassDetails.ClasspathDebugClassDetailsBuilder;
import io.openlineage.spark.agent.facets.DebugRunFacet.ClasspathDebugFacet;
import io.openlineage.spark.agent.facets.DebugRunFacet.LogicalPlanDebugFacet;
import io.openlineage.spark.agent.facets.DebugRunFacet.LogicalPlanNode;
import io.openlineage.spark.agent.facets.DebugRunFacet.LogicalPlanNode.LogicalPlanNodeBuilder;
import io.openlineage.spark.agent.facets.DebugRunFacet.MetricsDebugFacet;
import io.openlineage.spark.agent.facets.DebugRunFacet.MetricsNode;
import io.openlineage.spark.agent.facets.DebugRunFacet.SparkConfigDebugFacet;
import io.openlineage.spark.agent.facets.DebugRunFacet.SystemDebugFacet;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.DebugConfig;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang3.SystemUtils;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.Tuple2;

/** This class build the instance of DebugRunFacet and extracts debug */
public class DebugRunFacetBuilderDelegate {

  private final OpenLineageContext olContext;

  private static List<String> classesToDebug =
      Arrays.asList(
          "org.apache.spark.sql.delta.catalog.DeltaCatalog",
          "org.apache.iceberg.catalog.Catalog",
          "com.google.cloud.spark.bigquery.BigQueryRelation");

  private static final int DEFAULT_PAYLOAD_SIZE_LIMIT_IN_KILOBYTES = 100;

  public DebugRunFacetBuilderDelegate(OpenLineageContext olContext) {
    this.olContext = olContext;
  }

  public DebugRunFacet buildFacet() {
    return buildFacet(new ArrayList<>());
  }

  public DebugRunFacet buildFacet(List<String> logs) {
    int payloadSizeLimitInKilobytes =
        Optional.ofNullable(olContext.getOpenLineageConfig().getDebugConfig())
            .map(DebugConfig::getPayloadSizeLimitInKilobytes)
            .orElse(DEFAULT_PAYLOAD_SIZE_LIMIT_IN_KILOBYTES);
    return new DebugRunFacet(
        buildSparkConfigDebugFacet(),
        buildClasspathDebugFacet(),
        buildSystemDebugFacet(),
        buildLogicalPlanDebugFacet(),
        buildMetricsDebugFacet(),
        logs,
        payloadSizeLimitInKilobytes);
  }

  private SparkConfigDebugFacet buildSparkConfigDebugFacet() {
    return SparkConfigDebugFacet.builder()
        .extraListeners(getSparkConfOrNull("spark.extraListeners"))
        .openLineageConfig(olContext.getOpenLineageConfig())
        .catalogClass(getCatalogClass())
        .extensions(getSparkConfOrNull("spark.sql.extensions"))
        .build();
  }

  private ClasspathDebugFacet buildClasspathDebugFacet() {
    return ClasspathDebugFacet.builder()
        .openLineageVersion(
            toClasspathDebugClassDetails(this.getClass().getCanonicalName()).getPackageVersion())
        .sparkVersion(olContext.getSparkVersion())
        .scalaVersion(scala.util.Properties.versionNumberString())
        .jars(getSparkJars())
        .classDetails(
            classesToDebug.stream()
                .map(this::toClasspathDebugClassDetails)
                .collect(Collectors.toList()))
        .build();
  }

  private List<String> getSparkJars() {
    return olContext
        .getSparkContext()
        .map(sc -> sc.listJars())
        .map(jars -> ScalaConversionUtils.fromSeq(jars))
        .orElse(null);
  }

  private SystemDebugFacet buildSystemDebugFacet() {
    return SystemDebugFacet.builder()
        .sparkDeployMode(getDeployMode())
        .javaVersion(SystemUtils.JAVA_VERSION)
        .javaVendor(SystemUtils.JAVA_VENDOR)
        .osArch(SystemUtils.OS_ARCH)
        .osName(SystemUtils.OS_NAME)
        .osVersion(SystemUtils.OS_VERSION)
        .userLanguage(SystemUtils.USER_LANGUAGE)
        .userTimezone(SystemUtils.USER_TIMEZONE)
        .build();
  }

  private LogicalPlanDebugFacet buildLogicalPlanDebugFacet() {
    if (!olContext.hasLogicalPlan()) {
      return null;
    }
    return Optional.ofNullable(olContext.getLogicalPlan())
        .map(this::scanLogicalPlan)
        .map(LogicalPlanDebugFacet::new)
        .orElse(null);
  }

  private MetricsDebugFacet buildMetricsDebugFacet() {
    boolean metricsDisabled =
        Optional.ofNullable(olContext.getOpenLineageConfig().getDebugConfig())
            .map(DebugConfig::isMetricsDisabled)
            .orElse(false);

    if (metricsDisabled) {
      return null;
    }

    // We don't use other meters than gauge, counter and timer - add method here if you want to use
    // them.
    return new MetricsDebugFacet(
        olContext.getMeterRegistry().getMeters().stream()
            .map(
                meter ->
                    meter.match(
                        gauge ->
                            MetricsNode.builder()
                                .name(gauge.getId().getName())
                                .value(gauge.value())
                                .tags(gauge.getId().getTags())
                                .build(),
                        counter ->
                            MetricsNode.builder()
                                .name(counter.getId().getName())
                                .value(counter.count())
                                .tags(counter.getId().getTags())
                                .build(),
                        timer ->
                            MetricsNode.builder()
                                .name(timer.getId().getName())
                                .value(timer.totalTime(TimeUnit.MICROSECONDS))
                                .tags(timer.getId().getTags())
                                .build(),
                        null,
                        null,
                        null,
                        null,
                        null,
                        null))
            .map(MetricsNode.class::cast)
            .collect(Collectors.toList()));
  }

  private List<LogicalPlanNode> scanLogicalPlan(LogicalPlan node) {
    List<LogicalPlanNode> result = new ArrayList<>();
    LogicalPlanNodeBuilder builder =
        LogicalPlanNode.builder().id(nodeId(node)).desc(node.toString());

    if (node.children() != null) {
      builder.children(
          ScalaConversionUtils.fromSeq(node.children()).stream()
              .map(this::nodeId)
              .collect(Collectors.toList()));
    }
    result.add(builder.build());
    if (node.children() != null) {
      ScalaConversionUtils.fromSeq(node.children()).stream()
          .forEach(child -> result.addAll(scanLogicalPlan(child)));
    }
    return result;
  }

  private String nodeId(LogicalPlan node) {
    return String.format("%s@%s", node.nodeName(), node.hashCode());
  }

  private String getDeployMode() {
    return olContext.getSparkContext().map(sc -> sc.deployMode()).orElse(null);
  }

  private String getSparkConfOrNull(String confKey) {
    return olContext
        .getSparkContext()
        .map(sc -> sc.conf())
        .map(c -> c.get(confKey, null))
        .orElse(null);
  }

  private Map<String, String> getOpenLineageConfig() {
    return olContext
        .getSparkContext()
        .map(sc -> sc.conf())
        .map(conf -> conf.getAllWithPrefix("spark.openlineage."))
        .map(arr -> Arrays.stream(arr))
        .map(
            stream ->
                stream
                    .filter(tuple -> !tuple._1().toLowerCase(Locale.getDefault()).contains("key"))
                    .collect(
                        Collectors.<Tuple2<String, String>, String, String>toMap(
                            t -> t._1(), t -> t._2())))
        .orElse(Collections.emptyMap());
  }

  private String getCatalogClass() {
    return olContext
        .getSparkSession()
        .map(sparkSession -> sparkSession.catalog())
        .map(catalog -> catalog.getClass().getCanonicalName())
        .orElse(null);
  }

  private ClasspathDebugClassDetails toClasspathDebugClassDetails(String aClass) {
    ClasspathDebugClassDetailsBuilder builder = ClasspathDebugClassDetails.builder();
    builder.className(aClass);
    try {
      Class<?> clazz = this.getClass().getClassLoader().loadClass(aClass);
      String aClassPackage =
          Optional.ofNullable(clazz)
              .map(c -> c.getPackage())
              .map(p -> p.getImplementationVersion())
              .orElse(null);
      builder.isOnClasspath(true).packageVersion(aClassPackage);
    } catch (Exception e) {
      builder.isOnClasspath(false);
    }
    return builder.build();
  }
}
