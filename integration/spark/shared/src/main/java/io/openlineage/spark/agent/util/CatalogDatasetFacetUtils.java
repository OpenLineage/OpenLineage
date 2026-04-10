/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.filesystem.FilesystemDatasetUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;

public class CatalogDatasetFacetUtils {

  public static Optional<OpenLineage.CatalogDatasetFacet> getCatalogDatasetFacetForHive(
      OpenLineageContext context) {
    return context
        .getSparkContext()
        .flatMap(
            sparkContext -> {
              SparkConf sparkConf = sparkContext.getConf();
              Configuration hadoopConf = sparkContext.hadoopConfiguration();
              return PathUtils.getWarehouseLocation(sparkConf, hadoopConf)
                  .map(FilesystemDatasetUtils::fromLocation)
                  .map(FilesystemDatasetUtils::toLocation)
                  .map(location -> Pair.of(sparkContext, location));
            })
        .map(
            pair -> {
              OpenLineage.CatalogDatasetFacetBuilder builder =
                  context
                      .getOpenLineage()
                      .newCatalogDatasetFacetBuilder()
                      .name("default")
                      .framework("hive")
                      .type("hive")
                      .source("spark")
                      .catalogProperties(
                          getDataprocMetastoreProperties(pair.getLeft().getConf(), context));
              PathUtils.getMetastoreUri(pair.getLeft())
                  .map(PathUtils::prepareHiveUri)
                  .ifPresent(uri -> builder.metadataUri(uri.toString()));
              return builder.warehouseUri(pair.getRight().toString()).build();
            });
  }

  private static OpenLineage.CatalogDatasetFacetCatalogProperties getDataprocMetastoreProperties(
      SparkConf sparkConf, OpenLineageContext context) {
    Map<String, String> properties = new HashMap<>();
    SparkConfUtils.findSparkConfigKey(sparkConf, "spark.dataproc.metastore.project-id")
        .ifPresent(v -> properties.put("spark.dataproc.metastore.project-id", v));
    SparkConfUtils.findSparkConfigKey(sparkConf, "spark.dataproc.metastore.location")
        .ifPresent(v -> properties.put("spark.dataproc.metastore.location", v));
    SparkConfUtils.findSparkConfigKey(sparkConf, "spark.dataproc.metastore.instanceId")
        .ifPresent(v -> properties.put("spark.dataproc.metastore.instanceId", v));
    if (properties.isEmpty()) {
      return null;
    }
    OpenLineage.CatalogDatasetFacetCatalogPropertiesBuilder catalogPropertiesBuilder =
        context.getOpenLineage().newCatalogDatasetFacetCatalogPropertiesBuilder();
    properties.forEach(catalogPropertiesBuilder::put);
    return catalogPropertiesBuilder.build();
  }
}
