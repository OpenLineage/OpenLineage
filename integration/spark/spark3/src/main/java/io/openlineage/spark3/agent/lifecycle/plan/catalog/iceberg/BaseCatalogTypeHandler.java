/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.lifecycle.plan.catalog.CatalogUtils;
import io.openlineage.spark.agent.util.PathUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.jspecify.annotations.NonNull;

@Slf4j
abstract class BaseCatalogTypeHandler {

  abstract String getType();

  abstract boolean matchesCatalogType(Map<String, String> catalogConf);

  abstract Optional<DatasetIdentifier.Symlink> getSymlinkIdentifiers(
      SparkSession session, Map<String, String> catalogConf, String table);

  String getFacetType(Map<String, String> catalogConf) {
    return Optional.ofNullable(catalogConf.get(IcebergHandler.TYPE)).orElse(getType());
  }

  /**
   * Optionally supply the primary {@link DatasetIdentifier} directly, bypassing the default
   * table-location-based identity in {@code IcebergHandler.getDatasetIdentifier}. Handlers like S3
   * Tables override this so the user-facing identity matches the catalog (e.g. {@code
   * arn:aws:s3tables:...}) rather than an opaque physical bucket URI.
   */
  DatasetIdentifier getPrimaryIdentifier(
      SparkSession session,
      Map<String, String> catalogConf,
      Identifier identifier,
      TableCatalog tableCatalog) {
    String warehouseLocation = catalogConf.get(CatalogProperties.WAREHOUSE_LOCATION);

    Path path =
        getTableLocation(identifier, tableCatalog)
            .orElseGet(() -> defaultTableLocation(new Path(warehouseLocation), identifier));

    return PathUtils.fromPath(path);
  }

  protected @NonNull Optional<Path> getTableLocation(
      Identifier identifier, TableCatalog tableCatalog) {
    return getIcebergTable(tableCatalog, identifier).map(tbl -> new Path(tbl.location()));
  }

  Path defaultTableLocation(Path warehouseLocation, Identifier identifier) {
    // namespace1.namespace2.table -> /warehouseLocation/namespace1/namespace2/table
    String[] namespace = identifier.namespace();

    ArrayList<String> pathComponents = new ArrayList<>(namespace.length + 1);
    pathComponents.addAll(Arrays.asList(namespace));
    pathComponents.add(identifier.name());
    return new Path(warehouseLocation, String.join(Path.SEPARATOR, pathComponents));
  }

  Map<String, String> catalogProperties(Map<String, String> catalogConf) {
    return Collections.emptyMap();
  }

  @SneakyThrows
  protected Optional<Table> getIcebergTable(TableCatalog tableCatalog, Identifier identifier) {
    try {
      if (tableCatalog instanceof SparkCatalog) {
        SparkCatalog sparkCatalog = (SparkCatalog) tableCatalog;
        org.apache.spark.sql.connector.catalog.Table loadedTable =
            CatalogUtils.loadTable(sparkCatalog, identifier);

        // Handle different table implementations safely
        if (loadedTable instanceof SparkTable) {
          SparkTable sparkTable = (SparkTable) loadedTable;
          return Optional.ofNullable(sparkTable.table());
        } else {
          // Handle SparkChangelogTable and other unknown table types
          log.warn(
              "Loaded table is not a SparkTable instance. Table type: {}, identifier: {}. "
                  + "Attempting to extract Iceberg Table using reflection.",
              loadedTable.getClass().getName(),
              identifier);

          // Try to extract the underlying Iceberg Table using reflection
          // SparkChangelogTable and other wrappers typically have a table() method
          Optional<Table> reflectedTable = extractIcebergTableViaReflection(loadedTable);
          if (reflectedTable.isPresent()) {
            log.debug(
                "Successfully extracted Iceberg Table via reflection for identifier: {}",
                identifier);
            return reflectedTable;
          }

          log.warn(
              "Unable to extract Iceberg Table from table type: {} for identifier: {}. "
                  + "Returning empty to avoid ClassCastException.",
              loadedTable.getClass().getName(),
              identifier);
          return Optional.empty();
        }
      } else if (tableCatalog instanceof SparkSessionCatalog) {
        TableIdentifier tableIdentifier = TableIdentifier.parse(identifier.toString());
        SparkSessionCatalog sparkCatalog = (SparkSessionCatalog) tableCatalog;
        return Optional.ofNullable(
            CatalogUtils.loadTable(
                tableCatalog,
                identifier,
                "iceberg-table",
                () -> sparkCatalog.icebergCatalog().loadTable(tableIdentifier)));
      } else {
        log.warn(
            "Unknown catalog type: {} for identifier: {}. Expected SparkCatalog or SparkSessionCatalog.",
            tableCatalog.getClass().getName(),
            identifier);
        return Optional.empty();
      }
    } catch (ClassCastException e) {
      log.error(
          "ClassCastException while loading table from catalog. Catalog type: {}, identifier: {}",
          tableCatalog.getClass().getName(),
          identifier,
          e);
      return Optional.empty();
    } catch (Exception e) {
      if (e instanceof org.apache.spark.sql.catalyst.analysis.NoSuchTableException
          || e instanceof org.apache.iceberg.exceptions.NoSuchTableException) {
        // probably trying to obtain table details on START event while table does not exist
        log.debug("Table does not exist: {}", identifier);
        return Optional.empty();
      }
      log.error("Unexpected error while loading table: {}", identifier, e);
      throw e;
    }
  }

  /**
   * Attempts to extract an Iceberg Table from unknown table implementations using reflection. This
   * handles cases like SparkChangelogTable and future table types that wrap an Iceberg Table.
   *
   * @param table The loaded Spark table
   * @return Optional containing the Iceberg Table if successfully extracted, empty otherwise
   */
  private Optional<Table> extractIcebergTableViaReflection(
      org.apache.spark.sql.connector.catalog.Table table) {
    try {
      // Try to invoke table() method which is common across Iceberg table implementations
      java.lang.reflect.Method tableMethod = table.getClass().getMethod("table");
      Object result = tableMethod.invoke(table);

      if (result instanceof Table) {
        return Optional.of((Table) result);
      } else if (result != null) {
        log.warn(
            "table() method returned non-Table type: {} for table class: {}",
            result.getClass().getName(),
            table.getClass().getName());
      }
    } catch (NoSuchMethodException e) {
      log.debug(
          "No table() method found on table type: {}. This may not be an Iceberg table wrapper.",
          table.getClass().getName());
    } catch (Exception e) {
      log.warn(
          "Failed to extract Iceberg Table via reflection from table type: {}",
          table.getClass().getName(),
          e);
    }
    return Optional.empty();
  }
}
