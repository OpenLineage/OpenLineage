/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.utils;

import static io.openlineage.spark.agent.util.ScalaConversionUtils.asJavaOptional;

import io.openlineage.spark.agent.lifecycle.plan.catalog.CatalogUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg.IcebergRelationHandler;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg.IcebergSparkTables;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.delta.files.TahoeLogFileIndex;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;

@Slf4j
public final class DatasetVersionDatasetFacetUtils {
  private DatasetVersionDatasetFacetUtils() {}

  private static final String DELTA = "delta";

  /**
   * The version of an input dataset. An Iceberg input relation already holds the table the query
   * reads, so its current snapshot is taken from there instead of loading the table through the
   * catalog again on every event. Any other relation, or a failure reading the table, falls back to
   * the catalog lookup. Outputs keep the catalog lookup; they are not changed here.
   */
  public static Optional<String> extractInputVersionFromDataSourceV2Relation(
      OpenLineageContext context, DataSourceV2Relation table) {
    Optional<Optional<String>> fromRelation = Optional.empty();
    try {
      if (new IcebergRelationHandler(context).hasClasses()) {
        fromRelation = IcebergSparkTables.currentSnapshotId(table.table());
      }
    } catch (LinkageError e) {
      log.debug("Couldn't read the version from the relation's table, loading it instead", e);
    }
    return fromRelation.orElseGet(() -> extractVersionFromDataSourceV2Relation(context, table));
  }

  /**
   * Check if we have all the necessary properties in DataSourceV2Relation to get dataset version.
   */
  public static Optional<String> extractVersionFromDataSourceV2Relation(
      OpenLineageContext context, DataSourceV2Relation table) {
    if (table.identifier() != null
        && table.identifier().isDefined()
        && table.catalog() != null
        && table.catalog().isDefined()
        && table.catalog().get() instanceof TableCatalog) {
      TableCatalog tableCatalog = (TableCatalog) table.catalog().get();
      Optional<String> version;
      try {
        version =
            CatalogUtils.getDatasetVersion(
                context, tableCatalog, table.identifier().get(), table.table().properties());
      } catch (Exception | LinkageError e) {
        // A failing catalog call (e.g. a timeout or 5xx from a REST catalog) should only cost this
        // dataset its version, not drop it - or every other dataset of the plan - from the event.
        log.warn("Couldn't extract dataset version of table {}", table.identifier().get(), e);
        return Optional.empty();
      }
      if (version.isPresent()) {
        return version;
      }

      // A supported catalog that reports no version has genuinely none to report - the table has no
      // snapshot yet, which is the normal state on a START event. The owning catalog recovered from
      // the table name is that same catalog, so falling back would only repeat the table load.
      if (CatalogUtils.getCatalogHandler(context, tableCatalog).isPresent()) {
        return Optional.empty();
      }
    }

    // No CatalogHandler supports the relation's catalog - Iceberg's rewrite actions write through
    // SparkCachedTableCatalog or, on Spark 4.1 with Iceberg 1.11+, SparkRewriteTableCatalog - so
    // the version above came back empty. Fall back to the catalog that owns the table, the same one
    // the dataset identifier is resolved through.
    return owningCatalogVersion(context, table);
  }

  private static Optional<String> owningCatalogVersion(
      OpenLineageContext context, DataSourceV2Relation table) {
    try {
      return CatalogUtils.getOwningCatalogFromRelation(context, table)
          .flatMap(
              owner ->
                  CatalogUtils.getDatasetVersion(
                      context,
                      owner.getCatalog(),
                      owner.getIdentifier(),
                      table.table().properties()));
    } catch (Exception | LinkageError e) {
      log.warn("Couldn't extract dataset version of relation {}", table, e);
      return Optional.empty();
    }
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
      io.openlineage.spark3.agent.utils.DatasetVersionDatasetFacetUtils.class
          .getClassLoader()
          .loadClass("org.apache.spark.sql.delta.files.TahoeLogFileIndex");
      return true;
    } catch (NoClassDefFoundError | Exception e) {
      // If class does not exist or it's loading fails for some reason, we handle that failure by
      // returning false
    }
    return false;
  }
}
