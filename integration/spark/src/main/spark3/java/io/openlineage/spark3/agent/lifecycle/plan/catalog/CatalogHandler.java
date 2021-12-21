package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import io.openlineage.spark.agent.util.DatasetIdentifier;
import java.util.Map;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;

public interface CatalogHandler {
  boolean hasClasses();

  boolean isClass(TableCatalog tableCatalog);

  DatasetIdentifier getDatasetIdentifier(
      SparkSession session,
      TableCatalog tableCatalog,
      Identifier identifier,
      Map<String, String> properties);
}
