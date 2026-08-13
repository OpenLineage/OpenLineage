/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.utils;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.util.DatabricksUtils;
import io.openlineage.spark.agent.util.SparkSessionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Map;
import java.util.Optional;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.StructType;
import scala.Option;

/**
 * Resolves Unity Catalog table identity through the V2 catalog API when legacy {@code
 * getTableMetadata} is unavailable, matching the path used by {@code DataSourceV2Relation} and
 * {@code CreateReplaceOutputDatasetBuilder}.
 */
@Slf4j
public final class UnityCatalogTableDatasetUtils {

  private UnityCatalogTableDatasetUtils() {}

  @Value
  public static class ResolvedTableDataset {
    DatasetIdentifier datasetIdentifier;
    TableCatalog catalog;
    Identifier identifier;
    Map<String, String> properties;
    StructType schema;
  }

  public static Optional<ResolvedTableDataset> resolve(
      OpenLineageContext context, SparkSession session, TableIdentifier identifier) {
    Optional<String> catalogName = tableIdentifierCatalog(identifier);
    if (!catalogName.isPresent()) {
      catalogName = currentCatalog(session);
    }
    if (!catalogName.isPresent() || !identifier.database().isDefined()) {
      return Optional.empty();
    }

    String schema = identifier.database().get();
    String table = identifier.table();
    Optional<CatalogPlugin> catalogPlugin = SparkSessionUtils.catalog(session, catalogName.get());
    if (!catalogPlugin.isPresent() || !(catalogPlugin.get() instanceof TableCatalog)) {
      log.warn(
          "COPY INTO: catalog {} resolved to {}, which is not a TableCatalog",
          catalogName.get(),
          catalogPlugin.map(plugin -> plugin.getClass().getCanonicalName()).orElse("<empty>"));
      return Optional.empty();
    }

    TableCatalog tableCatalog = (TableCatalog) catalogPlugin.get();
    Identifier v2Identifier = Identifier.of(new String[] {schema}, table);
    try {
      Table loadedTable = tableCatalog.loadTable(v2Identifier);
      Map<String, String> properties = loadedTable.properties();
      Optional<DatasetIdentifier> datasetIdentifier =
          datasetIdentifier(context, tableCatalog, v2Identifier, properties);
      if (!datasetIdentifier.isPresent()) {
        log.warn(
            "COPY INTO: no catalog handler resolved {} from catalog {}",
            v2Identifier,
            tableCatalog.getClass().getCanonicalName());
        return Optional.empty();
      }
      return Optional.of(
          new ResolvedTableDataset(
              datasetIdentifier.get(),
              tableCatalog,
              v2Identifier,
              properties,
              loadedTable.schema()));
    } catch (NoSuchTableException e) {
      log.debug(
          "Unity Catalog table {}.{} not found in catalog {}", schema, table, catalogName.get());
      return Optional.empty();
    } catch (Exception e) {
      log.warn(
          "Unable to load Unity Catalog table {}.{} from catalog {}",
          schema,
          table,
          catalogName.get(),
          e);
      return Optional.empty();
    }
  }

  private static Optional<DatasetIdentifier> datasetIdentifier(
      OpenLineageContext context,
      TableCatalog catalog,
      Identifier identifier,
      Map<String, String> properties) {
    try {
      return PlanUtils3.getDatasetIdentifier(context, catalog, identifier, properties);
    } catch (Exception | NoSuchMethodError | NoClassDefFoundError e) {
      log.warn("Could not resolve dataset identifier of table {}", identifier, e);
      return unityCatalogIdentifier(context, catalog, identifier);
    }
  }

  private static Optional<DatasetIdentifier> unityCatalogIdentifier(
      OpenLineageContext context, TableCatalog catalog, Identifier identifier) {
    boolean unityCatalogEnabled =
        context
            .getSparkContext()
            .map(SparkContext::getConf)
            .map(DatabricksUtils::isDatabricksUnityCatalogEnabled)
            .orElse(false);
    if (!unityCatalogEnabled) {
      return Optional.empty();
    }
    String name = DatabricksUtils.qualifiedUnityCatalogTableName(catalog, identifier);
    return Optional.of(
        new DatasetIdentifier(name, DatabricksUtils.UNITY_CATALOG_SYMLINK_NAMESPACE));
  }

  private static Optional<String> tableIdentifierCatalog(TableIdentifier identifier) {
    try {
      Option<String> catalog = (Option<String>) MethodUtils.invokeMethod(identifier, "catalog");
      if (catalog != null && catalog.isDefined()) {
        return Optional.of(catalog.get()).filter(StringUtils::isNotBlank);
      }
    } catch (Exception e) {
      // TableIdentifier.catalog() is unavailable on older Spark versions
    }
    return Optional.empty();
  }

  private static Optional<String> currentCatalog(SparkSession session) {
    try {
      return Optional.ofNullable(
              (String) MethodUtils.invokeMethod(session.catalog(), "currentCatalog"))
          .filter(StringUtils::isNotBlank);
    } catch (Exception e) {
      return Optional.empty();
    }
  }
}
