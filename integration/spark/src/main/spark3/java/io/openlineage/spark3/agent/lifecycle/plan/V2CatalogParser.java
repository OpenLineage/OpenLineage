package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.spark.agent.util.PlanUtils;
import lombok.SneakyThrows;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog;

public class V2CatalogParser {
  public static Path getCatalogLocation(CatalogPlugin catalog) {
    //     TODO: add more catalogs (iceberg?)
    if (catalog instanceof JDBCTableCatalog) {
      return parseJdbc((JDBCTableCatalog) catalog);
    }
    return null;
  }

  @SneakyThrows
  private static Path parseJdbc(JDBCTableCatalog catalog) {
    JDBCOptions options = (JDBCOptions) FieldUtils.readField(catalog, "options", true);
    return new Path(PlanUtils.sanitizeJdbcUrl(options.url()));
  }
}
