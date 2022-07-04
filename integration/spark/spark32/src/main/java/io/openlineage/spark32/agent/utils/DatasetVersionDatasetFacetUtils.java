/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark32.agent.utils;

import static io.openlineage.spark.agent.util.ScalaConversionUtils.asJavaOptional;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark32.agent.lifecycle.plan.catalog.CatalogUtils3;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.delta.files.TahoeLogFileIndex;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;

@Slf4j
public class DatasetVersionDatasetFacetUtils {

  private static final String DELTA = "delta";

  /**
   * Check if we have all the necessary properties in DataSourceV2Relation to get dataset version.
   */
  public static Optional<String> extractVersionFromDataSourceV2Relation(
      DataSourceV2Relation table) {
    if (table.identifier().isEmpty()) {
      log.warn("Couldn't find identifier for dataset in plan " + table);
      return Optional.empty();
    }
    Identifier identifier = table.identifier().get();

    if (table.catalog().isEmpty() || !(table.catalog().get() instanceof TableCatalog)) {
      log.warn("Couldn't find catalog for dataset in plan " + table);
      return Optional.empty();
    }
    TableCatalog tableCatalog = (TableCatalog) table.catalog().get();

    Map<String, String> tableProperties = table.table().properties();
    return CatalogUtils3.getDatasetVersion(tableCatalog, identifier, tableProperties);
  }

  /**
   * Delta uses LogicalRelation's HadoopFsRelation as a logical plan's leaf. It implements FileIndex
   * using TahoeLogFileIndex that contains DeltaLog, which can be used to get dataset's snapshot.
   */
  public static Optional<String> extractVersionFromLogicalRelation(
      LogicalRelation logicalRelation) {
    if (logicalRelation.relation() instanceof HadoopFsRelation) {
      HadoopFsRelation fsRelation = (HadoopFsRelation) logicalRelation.relation();
      asJavaOptional(logicalRelation.catalogTable());
      if (logicalRelation.catalogTable().isDefined()
          && logicalRelation.catalogTable().get().provider().isDefined()
          && DELTA.equalsIgnoreCase(logicalRelation.catalogTable().get().provider().get())) {
        if (hasDeltaClasses() && fsRelation.location() instanceof TahoeLogFileIndex) {
          TahoeLogFileIndex fileIndex = (TahoeLogFileIndex) fsRelation.location();
          return Optional.of(Long.toString(fileIndex.getSnapshot().version()));
        }
      }
    }
    return Optional.empty();
  }

  protected static boolean hasDeltaClasses() {
    try {
      DatasetVersionDatasetFacetUtils.class
          .getClassLoader()
          .loadClass("org.apache.spark.sql.delta.files.TahoeLogFileIndex");
      return true;
    } catch (Exception e) {
      // swallow- we don't care
    }
    return false;
  }

  public static void includeDatasetVersion(
      OpenLineageContext context,
      OpenLineage.DatasetFacetsBuilder datasetFacetsBuilder,
      DataSourceV2Relation relation) {
    DatasetVersionDatasetFacetUtils.extractVersionFromDataSourceV2Relation(relation)
        .ifPresent(
            version ->
                datasetFacetsBuilder.version(
                    context.getOpenLineage().newDatasetVersionDatasetFacet(version)));
  }
}
