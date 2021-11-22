package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import io.openlineage.spark.agent.util.DatasetIdentifier;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;

public class CatalogUtils3 {

  private static List<CatalogParser> parsers =
      Arrays.asList(new IcebergParser(), new DeltaParser(), new JdbcParser(), new V2Parser());

  public static DatasetIdentifier getDatasetIdentifier(
      SparkSession session,
      TableCatalog catalog,
      Identifier identifier,
      Map<String, String> properties) {
    for (CatalogParser parser : parsers) {
      if (parser.hasClasses() && parser.isClass(catalog)) {
        return parser.getDatasetIdentifier(session, catalog, identifier, properties);
      }
    }
    throw new UnsupportedCatalogException(catalog.getClass().getCanonicalName());
  }
}
