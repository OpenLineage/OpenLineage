package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import io.openlineage.spark.agent.util.DatasetIdentifier;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;

public class CatalogUtils3 {

  private static List<CatalogHandler> parsers = getHandlers();

  private static List<CatalogHandler> getHandlers() {
    List<CatalogHandler> handlers =
        Arrays.asList(
            new IcebergHandler(),
            new DeltaHandler(),
            new JdbcHandler(),
            new V2SessionCatalogHandler());
    return handlers.stream().filter(CatalogHandler::hasClasses).collect(Collectors.toList());
  }

  public static DatasetIdentifier getDatasetIdentifier(
      SparkSession session,
      TableCatalog catalog,
      Identifier identifier,
      Map<String, String> properties) {
    for (CatalogHandler parser : parsers) {
      if (parser.isClass(catalog)) {
        return parser.getDatasetIdentifier(session, catalog, identifier, properties);
      }
    }
    throw new UnsupportedCatalogException(catalog.getClass().getCanonicalName());
  }
}
