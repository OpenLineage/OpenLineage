/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import io.openlineage.spark.agent.facets.TableProviderFacet;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;

public class CatalogUtils3 {

  private static List<CatalogHandler> catalogHandlers = getHandlers();

  private static List<CatalogHandler> getHandlers() {
    List<CatalogHandler> handlers =
        Arrays.asList(
            new IcebergHandler(),
            new DeltaHandler(),
            new DatabricksDeltaHandler(),
            new JdbcHandler(),
            new V2SessionCatalogHandler());
    return handlers.stream().filter(CatalogHandler::hasClasses).collect(Collectors.toList());
  }

  public static DatasetIdentifier getDatasetIdentifier(
      SparkSession session,
      TableCatalog catalog,
      Identifier identifier,
      Map<String, String> properties) {
    return getDatasetIdentifier(session, catalog, identifier, properties, catalogHandlers);
  }

  public static DatasetIdentifier getDatasetIdentifier(
      SparkSession session,
      TableCatalog catalog,
      Identifier identifier,
      Map<String, String> properties,
      List<CatalogHandler> handlers) {

    return handlers.stream()
        .filter(handler -> handler.isClass(catalog))
        .map(handler -> handler.getDatasetIdentifier(session, catalog, identifier, properties))
        .findAny()
        .orElseThrow(() -> new UnsupportedCatalogException(catalog.getClass().getCanonicalName()));
  }

  public static Optional<CatalogHandler> getCatalogHandler(TableCatalog catalog) {
    return catalogHandlers.stream().filter(handler -> handler.isClass(catalog)).findAny();
  }

  public static Optional<TableProviderFacet> getTableProviderFacet(
      TableCatalog catalog, Map<String, String> properties) {
    Optional<CatalogHandler> catalogHandler = getCatalogHandler(catalog);
    return catalogHandler.isPresent()
        ? catalogHandler.get().getTableProviderFacet(properties)
        : Optional.empty();
  }

  public static Optional<String> getDatasetVersion(
      TableCatalog catalog, Identifier identifier, Map<String, String> properties) {
    Optional<CatalogHandler> catalogHandler = getCatalogHandler(catalog);
    return catalogHandler.isPresent()
        ? catalogHandler.get().getDatasetVersion(catalog, identifier, properties)
        : Optional.empty();
  }

  public static Optional<CatalogHandler> getCatalogHandlerByProvider(String provider) {
    return catalogHandlers.stream()
        .filter(handler -> handler.getName().equalsIgnoreCase(provider))
        .findAny();
  }
}
