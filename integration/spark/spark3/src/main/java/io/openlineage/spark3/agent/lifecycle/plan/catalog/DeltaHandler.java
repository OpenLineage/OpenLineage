/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.V1Table;
import org.apache.spark.sql.delta.Snapshot;
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
      Optional<Snapshot> snapshot = getDeltaTableSnapshot(deltaTable);
      if (snapshot.isPresent()) {
        return Optional.of(Long.toString(snapshot.get().version()));
      }
    }
    return Optional.empty();
  }

  /**
   * Versions of Delta differ in implementation of {@link DeltaTableV2} class. This method retrieves
   * the table snapshot regardless of the Delta version. Previously `snapshot` method was available
   * in Scala class for Delta < 3. Recent changes in Delta 3+ changed the naming of this function to
   * be 'initialSnapshot'. The developers wanted to indicate with this rename that the snapshot we
   * are getting, is a lazy value that returns the initial version you read. If the table has been
   * changed in the meantime, you will not get the latest snapshot but that initial version you
   * read.
   *
   * @return Optional SnapShot of the deltaTable.
   */
  private Optional<Snapshot> getDeltaTableSnapshot(DeltaTableV2 deltaTable) {
    if (MethodUtils.getAccessibleMethod(deltaTable.getClass(), "snapshot") != null) {
      try {
        return Optional.of((Snapshot) MethodUtils.invokeMethod(deltaTable, "snapshot"));
      } catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
        log.error("Could not invoke method", e);
      }
    } else if (MethodUtils.getAccessibleMethod(deltaTable.getClass(), "initialSnapshot") != null) {
      try {
        return Optional.of((Snapshot) MethodUtils.invokeMethod(deltaTable, "initialSnapshot"));
      } catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
        log.error("Could not invoke method", e);
      }
    }
    return Optional.empty();
  }

  @Override
  public String getName() {
    return "delta";
  }
}
