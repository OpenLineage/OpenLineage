/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.filesystem.FilesystemDatasetUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.NoSuchElementException;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.execution.datasources.v2.V2SessionCatalog;
import org.apache.spark.sql.hive.HiveSessionCatalog;
import scala.Option;

@Slf4j
public class CatalogDatasetFacetUtils {

  public static Optional<OpenLineage.CatalogDatasetFacet> getCatalogDatasetFacetForHive(
      OpenLineageContext context) {
    return context
        .getSparkContext()
        .flatMap(
            sparkContext -> {
              OpenLineage.CatalogDatasetFacetBuilder builder =
                  context
                      .getOpenLineage()
                      .newCatalogDatasetFacetBuilder()
                      .framework("hive")
                      .source("spark");
              Optional<URI> warehouseUri;
              if (GoogleCloudPlatformUtils.isBigLakeHiveCatalog(sparkContext.conf())) {
                Option<String> catalogName =
                    sparkContext.getConf().getOption("spark.hive.metastore.blms.catalog.default");
                if (!catalogName.isDefined()) {
                  log.warn(
                      "Big Lake Hive Catalog detected, but no default catalog configured. Please set spark.hive.metastore.blms.catalog.default to enable catalog dataset facets");
                  return Optional.empty();
                }
                warehouseUri =
                    SparkConfUtils.findHadoopConfigKey(
                            sparkContext.hadoopConfiguration(), "hive.metastore.warehouse.dir")
                        .map(URI::create);
                builder.name(catalogName.get()).type("gcp_lakehouse");
                sparkContext
                    .getConf()
                    .getOption("spark.hive.metastore.blms.project.id")
                    .foreach(
                        projectId ->
                            builder.catalogProperties(
                                context
                                    .getOpenLineage()
                                    .newCatalogDatasetFacetCatalogPropertiesBuilder()
                                    .put("gcp_project_id", projectId)
                                    .build()));
              } else {
                warehouseUri =
                    PathUtils.getWarehouseLocation(
                        sparkContext.getConf(), sparkContext.hadoopConfiguration());
                builder.name("default").type("hive");
                PathUtils.getMetastoreUri(sparkContext)
                    .map(PathUtils::prepareHiveUri)
                    .ifPresent(uri -> builder.metadataUri(uri.toString()));
              }

              if (!warehouseUri.isPresent()) {
                log.warn(
                    "Unable to determine warehouse URI for Hive catalog, skipping catalog dataset facet");
              }
              return warehouseUri
                  .map(FilesystemDatasetUtils::fromLocation)
                  .map(FilesystemDatasetUtils::toLocation)
                  .map(location -> builder.warehouseUri(location.toString()).build());
            });
  }

  public static boolean isHiveCatalog(SparkSession session, TableIdentifier identifier) {
    return "hive".equals(session.sparkContext().conf().get("spark.sql.catalogImplementation", ""))
        && getCatalogPlugin(session, identifier)
            .map(catalogPlugin -> isHiveCatalog(session.sparkContext(), catalogPlugin))
            .orElse(false);
  }

  @SuppressWarnings("PMD")
  private static boolean isHiveCatalog(SparkContext context, CatalogPlugin catalogPlugin) {
    if (catalogPlugin instanceof V2SessionCatalog) {
      V2SessionCatalog v2Catalog = ((V2SessionCatalog) catalogPlugin);
      Field catalog = FieldUtils.getField(V2SessionCatalog.class, "catalog", true);
      catalog.setAccessible(true);
      try {
        return catalog.get(v2Catalog) instanceof HiveSessionCatalog;
      } catch (IllegalAccessException e) {
        return false;
      }
    }
    return false;
  }

  public static Optional<CatalogPlugin> getCatalogPlugin(
      SparkSession session, TableIdentifier identifier) {
    try {
      //noinspection unchecked
      return Optional.ofNullable(
              ((Option<String>) MethodUtils.invokeMethod(identifier, "catalog")).get())
          .map(catalogName -> session.sessionState().catalogManager().catalog(catalogName));
    } catch (NoSuchMethodException
        | IllegalAccessException
        | InvocationTargetException
        | NoSuchElementException e) {
      //            log.debug("No catalog name, cannot add catalog/storage facets");
      return Optional.empty();
    }
  }
}
