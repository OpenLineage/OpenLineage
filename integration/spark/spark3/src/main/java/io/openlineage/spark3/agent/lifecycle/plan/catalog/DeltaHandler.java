/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Map;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.V1Table;
import org.apache.spark.sql.delta.catalog.DeltaCatalog;
import org.apache.spark.sql.delta.catalog.DeltaTableV2;

@Slf4j
public class DeltaHandler implements CatalogHandler {
  private final OpenLineageContext context;

  public DeltaHandler(OpenLineageContext context) {
    this.context = context;
  }

  @Override
  public boolean hasClasses() {
    try {
      DeltaHandler.class
          .getClassLoader()
          .loadClass("org.apache.spark.sql.delta.catalog.DeltaCatalog");
      return true;
    } catch (Exception e) {
      // swallow- we don't care
    }
    return false;
  }

  @Override
  public boolean isClass(TableCatalog tableCatalog) {
    return tableCatalog instanceof DeltaCatalog;
  }

  @Override
  public DatasetIdentifier getDatasetIdentifier(
      SparkSession session,
      TableCatalog tableCatalog,
      Identifier identifier,
      Map<String, String> properties) {
    DeltaCatalog catalog = (DeltaCatalog) tableCatalog;

    Table table = catalog.loadTable(identifier);
    if (catalog.isPathIdentifier(identifier)) {
      // no information in metastore, only path
      Path path = new Path(identifier.name());
      return PathUtils.fromPath(path);
    }

    if (table instanceof DeltaTableV2) {
      DeltaTableV2 deltaTable = (DeltaTableV2) table;
      // catalogTable is Option, but it is empty only for path identifier
      CatalogTable catalogTable = deltaTable.catalogTable().get();
      return PathUtils.fromCatalogTable(catalogTable, session);
    }

    // not a Delta table, fallback to SparkCatalog. See:
    // https://github.com/delta-io/delta/blob/v3.2.0/spark/src/main/scala/org/apache/spark/sql/delta/catalog/DeltaCatalog.scala#L193-L199
    V1Table v1Table = (V1Table) table;
    return PathUtils.fromCatalogTable(v1Table.catalogTable(), session);
  }

  @Override
  public Optional<OpenLineage.StorageDatasetFacet> getStorageDatasetFacet(
      Map<String, String> properties) {
    return Optional.of(
        context
            .getOpenLineage()
            .newStorageDatasetFacet("delta", "parquet")); // Delta is always parquet
  }

  @SneakyThrows
  @Override
  public Optional<String> getDatasetVersion(
      TableCatalog tableCatalog, Identifier identifier, Map<String, String> properties) {
    DeltaCatalog deltaCatalog = (DeltaCatalog) tableCatalog;
    Table table = deltaCatalog.loadTable(identifier);

    if (table instanceof DeltaTableV2) {
      DeltaTableV2 deltaTable = (DeltaTableV2) table;
      return Optional.of(Long.toString(deltaTable.snapshot().version()));
    }
    return Optional.empty();
  }

  @Override
  public String getName() {
    return "delta";
  }
}
