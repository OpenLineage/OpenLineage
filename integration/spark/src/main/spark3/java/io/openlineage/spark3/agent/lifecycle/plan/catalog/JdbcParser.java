package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.agent.util.PlanUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog;

public class JdbcParser implements CatalogParser {

  public static boolean isJdbcCatalog(TableCatalog catalog) {
    return catalog instanceof JDBCTableCatalog;
  }

  @Override
  public boolean hasClasses() {
    return true;
  }

  @Override
  public boolean isClass(TableCatalog tableCatalog) {
    return tableCatalog instanceof JDBCTableCatalog;
  }

  @SneakyThrows
  @Override
  public DatasetIdentifier getDatasetIdentifier(
      SparkSession session,
      TableCatalog tableCatalog,
      Identifier identifier,
      Map<String, String> properties) {
    JDBCTableCatalog catalog = (JDBCTableCatalog) tableCatalog;
    JDBCOptions options = (JDBCOptions) FieldUtils.readField(catalog, "options", true);

    List<String> nameElements = new ArrayList<>(Arrays.asList(identifier.namespace()));
    nameElements.add(identifier.name());

    return new DatasetIdentifier(
        String.join(".", nameElements), PlanUtils.sanitizeJdbcUrl(options.url()));
  }
}
