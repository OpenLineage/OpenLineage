/*
/* Copyright 2018-2023 contributors to the OpenLineage project
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
import io.openlineage.spark.agent.facets.DebugRunFacet.SparkConfigDebugFacet;
import io.openlineage.spark.agent.facets.DebugRunFacet.SystemDebugFacet;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

  public DebugRunFacetBuilderDelegate(OpenLineageContext olContext) {
    this.olContext = olContext;
  }

  protected DebugRunFacet buildFacet() {
    return new DebugRunFacet(
        buildSparkConfigDebugFacet(),
        buildClasspathDebugFacet(),
        buildSystemDebugFacet(),
        buildLogicalPlanDebugFacet());
  }

  private SparkConfigDebugFacet buildSparkConfigDebugFacet() {
    return SparkConfigDebugFacet.builder()
        .extraListeners(getSparkConfOrNull("spark.extraListeners"))
        .openLineageConfig(getOpenLineageConfig())
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
    return Optional.ofNullable(olContext.getSparkContext())
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
    return olContext
        .getQueryExecution()
        .map(qe -> qe.optimizedPlan())
        .map(plan -> scanLogicalPlan(plan))
        .map(list -> new LogicalPlanDebugFacet(list))
        .orElse(null);
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
    return Optional.ofNullable(olContext.getSparkContext()).map(sc -> sc.deployMode()).orElse(null);
  }

  private String getSparkConfOrNull(String confKey) {
    return Optional.ofNullable(olContext.getSparkContext())
        .map(sc -> sc.conf())
        .map(c -> c.get(confKey, null))
        .orElse(null);
  }

  private Map<String, String> getOpenLineageConfig() {
    return Optional.ofNullable(olContext.getSparkContext())
        .map(sc -> sc.conf())
        .map(conf -> conf.getAllWithPrefix("spark.openlineage."))
        .map(arr -> Arrays.stream(arr))
        .map(
            stream ->
                stream
                    .filter(tuple -> !tuple._1().toLowerCase().contains("key"))
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

  private boolean isOnClassPath(String aClass) {
    try {
      this.getClass().getClassLoader().loadClass(aClass);
      return true;
    } catch (Exception e) {
      return false;
    }
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
