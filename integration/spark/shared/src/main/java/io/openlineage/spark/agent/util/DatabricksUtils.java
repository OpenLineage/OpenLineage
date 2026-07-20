/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.spark.api.OpenLineageContext;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Option;

/** Utils method to help exctact values from Databricks environmemt */
@Slf4j
public class DatabricksUtils {

  public static final String SPARK_DATABRICKS_WORKSPACE_URL = "spark.databricks.workspaceUrl";
  public static final String DATABRICKS_RUNTIME_VERSION = "DATABRICKS_RUNTIME_VERSION";
  public static final String UNITY_CATALOG_SYMLINK_NAMESPACE = "unity-catalog";

  /**
   * Determines if a Spark job is run on Databricks platform
   *
   * @return
   */
  public static boolean isRunOnDatabricksPlatform(OpenLineageContext context) {
    return getWorkspaceUrl(context).isPresent();
  }

  /**
   * Determines if a Spark job is run on Databricks platform. Fast check using
   * DATABRICKS_RUNTIME_VERSION environment variable first, falls back to checking SparkConf for
   * workspace URL.
   */
  public static boolean isRunOnDatabricksPlatform(SparkConf conf) {
    return System.getenv().containsKey(DATABRICKS_RUNTIME_VERSION)
        || conf.contains(SPARK_DATABRICKS_WORKSPACE_URL);
  }

  public static boolean isDatabricksUnityCatalogEnabled(SparkConf conf) {
    return isRunOnDatabricksPlatform(conf)
        && "true".equals(conf.get("spark.databricks.unityCatalog.enabled", "false"));
  }

  public static Optional<String> getWorkspaceUrl(OpenLineageContext context) {
    return context
        .getSparkSession()
        .map(SparkSession::sparkContext)
        .map(SparkContext::getConf)
        .filter(DatabricksUtils::isRunOnDatabricksPlatform)
        .map(conf -> conf.get(SPARK_DATABRICKS_WORKSPACE_URL));
  }

  public static Optional<String> getWorkspaceUrl(SparkConf conf) {
    return Optional.ofNullable(conf.get(SPARK_DATABRICKS_WORKSPACE_URL));
  }

  public static String prettifyDatabricksJobName(SparkConf conf, String jobName) {
    // replace default job name with workspace id when no app name specified;
    return jobName.replace(
        "databricks_shell.", // default name
        extractWorkspaceId(DatabricksUtils.getWorkspaceUrl(conf).get() + "."));
  }

  private static String extractWorkspaceId(String workspaceUrl) {
    return workspaceUrl
        .replace(".cloud.databricks.com/", "") // extract workspace id from workspaceUrl
        .replace("https://", "");
  }

  /**
   * Builds a Unity Catalog-qualified table name from a Spark {@link
   * org.apache.spark.sql.catalyst.TableIdentifier}.
   */
  public static String qualifiedUnityCatalogTableName(
      org.apache.spark.sql.catalyst.TableIdentifier identifier) {
    StringBuilder name = new StringBuilder();
    getTableIdentifierCatalog(identifier).ifPresent(catalog -> name.append(catalog).append("."));
    if (identifier.database().isDefined()) {
      name.append(identifier.database().get()).append(".");
    }
    name.append(identifier.table());
    return name.toString();
  }

  private static Optional<String> getTableIdentifierCatalog(
      org.apache.spark.sql.catalyst.TableIdentifier identifier) {
    try {
      Option<String> catalog = (Option<String>) MethodUtils.invokeMethod(identifier, "catalog");
      if (catalog != null && catalog.isDefined()) {
        return Optional.of(catalog.get());
      }
    } catch (Exception e) {
      // TableIdentifier.catalog() is unavailable on older Spark versions
    }
    return Optional.empty();
  }

  /**
   * Builds a Unity Catalog-qualified table name from a V2 catalog {@link
   * org.apache.spark.sql.connector.catalog.Identifier}.
   */
  public static String qualifiedUnityCatalogTableName(
      org.apache.spark.sql.connector.catalog.TableCatalog tableCatalog,
      org.apache.spark.sql.connector.catalog.Identifier identifier) {
    String name = identifier.toString();
    String catalogName = tableCatalog.name();
    if (catalogName != null && !catalogName.isEmpty() && !name.startsWith(catalogName + ".")) {
      return catalogName + "." + name;
    }
    return name;
  }
}
