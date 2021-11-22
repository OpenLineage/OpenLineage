package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.agent.util.SparkConfUtils;
import java.net.URI;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;

@Slf4j
public class IcebergParser implements CatalogParser {
  @Override
  public boolean hasClasses() {
    try {
      IcebergParser.class.getClassLoader().loadClass("org.apache.iceberg.catalog.Catalog");
      return true;
    } catch (Exception e) {
      // swallow- we don't care
    }
    return false;
  }

  @Override
  public boolean isClass(TableCatalog tableCatalog) {
    return tableCatalog instanceof SparkCatalog;
  }

  @Override
  public DatasetIdentifier getDatasetIdentifier(
      SparkSession session,
      TableCatalog tableCatalog,
      Identifier identifier,
      Map<String, String> properties) {
    String catalogName = ((SparkCatalog) tableCatalog).name();
    String prefix = String.format("spark.sql.catalog.%s", catalogName);
    Map<String, String> conf =
        ScalaConversionUtils.<String, String>fromMap(session.conf().getAll());
    log.warn(conf.toString());
    Map<String, String> catalogConf =
        conf.entrySet().stream()
            .filter(x -> x.getKey().startsWith(prefix))
            .collect(
                Collectors.toMap(
                    x -> x.getKey().substring(prefix.length() + 1), // handle dot after prefix
                    Map.Entry::getValue));

    log.warn(catalogConf.toString());
    if (catalogConf.isEmpty() || !catalogConf.containsKey("type")) {
      throw new UnsupportedCatalogException(catalogName);
    }
    log.warn(catalogConf.get("type"));
    switch (catalogConf.get("type")) {
      case "hadoop":
        return getHadoop(catalogConf, identifier.toString());
      case "hive":
        return getHive(session, catalogConf, identifier.toString());
      default:
        throw new UnsupportedCatalogException(catalogConf.get("type"));
    }
  }

  private DatasetIdentifier getHadoop(Map<String, String> conf, String table) {
    String warehouse = conf.get(CatalogProperties.WAREHOUSE_LOCATION);
    return PathUtils.fromPath(new Path(warehouse, table));
  }

  @SneakyThrows
  private DatasetIdentifier getHive(SparkSession session, Map<String, String> conf, String table) {
    String confUri = conf.get(CatalogProperties.URI);
    if (confUri == null) {
      confUri =
          SparkConfUtils.getMetastoreKey(session.sparkContext().conf())
              .orElseThrow(() -> new UnsupportedCatalogException("hive"));
    }
    URI uri = new URI(confUri);
    log.warn(uri.toString());
    // Recreate conf URI with hive scheme, and table as path
    uri =
        new URI(
            "hive", null, uri.getHost(), uri.getPort(), String.format("/%s", table), null, null);
    return PathUtils.fromURI(uri, "hive");
  }
}
