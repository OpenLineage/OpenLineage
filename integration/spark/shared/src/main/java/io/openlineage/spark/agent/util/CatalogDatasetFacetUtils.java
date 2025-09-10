/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.filesystem.FilesystemDatasetUtils;
import io.openlineage.spark.api.OpenLineageContext;
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
                      .framework("hive")
                      .type("hive")
                      .source("spark");
              PathUtils.getMetastoreUri(pair.getLeft())
                  .map(PathUtils::prepareHiveUri)
                  .ifPresent(uri -> builder.metadataUri(uri.toString()));
              return builder.warehouseUri(pair.getRight().toString()).build();
            });
  }
}
