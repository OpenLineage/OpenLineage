package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.agent.util.SparkConfUtils;
import java.net.URI;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;

@Slf4j
public class IcebergHandler implements CatalogHandler {
  @Override
  public boolean hasClasses() {
    try {
      IcebergHandler.class.getClassLoader().loadClass("org.apache.iceberg.catalog.Catalog");
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
            .filter(x -> x.getKey().length() > prefix.length())
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
        return getHadoopIdentifier(catalogConf, identifier.toString());
      case "hive":
        return getHiveIdentifier(
            session, catalogConf.get(CatalogProperties.URI), identifier.toString());
      default:
        throw new UnsupportedCatalogException(catalogConf.get("type"));
    }
  }

  private DatasetIdentifier getHadoopIdentifier(Map<String, String> conf, String table) {
    String warehouse = conf.get(CatalogProperties.WAREHOUSE_LOCATION);
    return PathUtils.fromPath(new Path(warehouse, table));
  }

  @SneakyThrows
  private DatasetIdentifier getHiveIdentifier(
      SparkSession session, @Nullable String confUri, String table) {
    table = String.format("/%s", table);
    URI uri;
    if (confUri == null) {
      uri =
          SparkConfUtils.getMetastoreUri(session.sparkContext().conf())
              .orElseThrow(() -> new UnsupportedCatalogException("hive"));
    } else {
      uri = new URI(confUri);
    }
    return PathUtils.fromPath(new Path(PathUtils.enrichHiveMetastoreURIWithTableName(uri, table)));
  }
}
