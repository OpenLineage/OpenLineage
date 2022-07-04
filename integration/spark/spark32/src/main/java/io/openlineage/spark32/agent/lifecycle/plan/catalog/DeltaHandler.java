/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark32.agent.lifecycle.plan.catalog;

import io.openlineage.spark.agent.facets.TableProviderFacet;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.agent.util.PathUtils;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.delta.catalog.DeltaCatalog;
import org.apache.spark.sql.delta.catalog.DeltaTableV2;
import scala.Option;

@Slf4j
public class DeltaHandler implements CatalogHandler {

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

    Optional<String> location;
    if (catalog.isPathIdentifier(identifier)) {
      location = Optional.of(identifier.name());
    } else {
      location = Optional.ofNullable(properties.get("location"));
    }
    // Delta uses spark2 catalog when location isn't specified.
    Path path =
        new Path(
            location.orElse(
                session
                    .sessionState()
                    .catalog()
                    .defaultTablePath(
                        TableIdentifier.apply(
                            identifier.name(),
                            Option.apply(
                                Arrays.stream(identifier.namespace())
                                    .reduce((x, y) -> y)
                                    .orElse(null))))
                    .toString()));
    log.info(path.toString());
    return PathUtils.fromPath(path, "file");
  }

  @Override
  public Optional<TableProviderFacet> getTableProviderFacet(Map<String, String> properties) {
    return Optional.of(new TableProviderFacet("delta", "parquet")); // Delta is always parquet
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
