package io.openlineage.spark.agent.util;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;

@Slf4j
public class PathUtils {

  private static final String DEFAULT_SCHEME = "file";

  public static DatasetIdentifier fromPath(Path path) {
    return PathUtils.fromPath(path, DEFAULT_SCHEME);
  }

  public static DatasetIdentifier fromPath(Path path, String defaultScheme) {
    if (path.isAbsoluteAndSchemeAuthorityNull()) {
      return new DatasetIdentifier(path.toString(), defaultScheme);
    }
    URI uri = path.toUri();
    String namespace =
        Optional.ofNullable(uri.getAuthority())
            .map(a -> String.format("%s://%s", uri.getScheme(), a))
            .orElseGet(
                () -> {
                  if (uri.getScheme() != null) {
                    return uri.getScheme();
                  } else {
                    return defaultScheme;
                  }
                });
    String name = fixName(uri.getPath());
    return new DatasetIdentifier(name, namespace);
  }

  public static DatasetIdentifier fromURI(URI location, String defaultScheme) {
    return fromPath(new Path(location), defaultScheme);
  }

  /**
   * Create DatasetIdentifier from location. If the location does not exist, use provided authority
   * with hive scheme.
   */
  public static DatasetIdentifier fromCatalogTable(CatalogTable catalogTable, String authority) {
    try {
      URI location = catalogTable.location();
      return PathUtils.fromURI(location, "file");
    } catch (Exception e) { // Java does not recognize scala exception
      if (e instanceof AnalysisException) {
        try {
          String qualifiedName = catalogTable.qualifiedName();
          if (!qualifiedName.startsWith("/")) {
            qualifiedName = String.format("/%s", qualifiedName);
          }
          return PathUtils.fromPath(
              new Path(new URI("hive", authority, qualifiedName, null, null)));
        } catch (URISyntaxException uriSyntaxException) {
          throw new IllegalArgumentException(uriSyntaxException);
        }
      }
      throw e;
    }
  }

  /**
   * Create DatasetIdentifier from CatalogTable, using storage's locationURI if it exists. In other
   * way, use defaultTablePath.
   */
  @SneakyThrows
  public static DatasetIdentifier fromCatalogTable(CatalogTable catalogTable) {
    String authority =
        ScalaConversionUtils.asJavaOptional(catalogTable.storage().locationUri())
            .map(URI::toString)
            .orElseGet(
                () ->
                    SparkSession.active()
                        .sessionState()
                        .catalog()
                        .defaultTablePath(catalogTable.identifier())
                        .toString());
    return PathUtils.fromCatalogTable(catalogTable, authority);
  }

  /**
   * We use Hive metastore's address as a namespace for tables, with a physical location of a table
   * as a backup. The physical location, when existing, will be also included as a separate facet,
   * to facilicate "symlinking" of those datasets on OpenLineage consumer.
   */
  @SneakyThrows
  public static DatasetIdentifier fromHiveTable(
      SparkSession sparkSession, CatalogTable catalogTable) {
    Optional<URI> metastoreUri =
        SparkConfUtils.getMetastoreUri(sparkSession.sparkContext().getConf());
    if (metastoreUri.isPresent()) {
      URI uri = metastoreUri.get();
      String qualifiedName = catalogTable.qualifiedName();
      if (!qualifiedName.startsWith("/")) {
        qualifiedName = String.format("/%s", qualifiedName);
      }
      return PathUtils.fromHiveTable(qualifiedName, uri);
    }
    return PathUtils.fromURI(catalogTable.location(), "file");
  };

  @SneakyThrows
  public static DatasetIdentifier fromHiveTable(String qualifiedName, URI metastoreUri) {
    return PathUtils.fromPath(
        new Path(
            new URI(
                "hive",
                null,
                metastoreUri.getHost(),
                metastoreUri.getPort(),
                qualifiedName,
                null,
                null)));
  }

  private static String fixName(String name) {
    if (name.chars().filter(x -> x == '/').count() > 1) {
      return name;
    }
    if (name.startsWith("/")) {
      return name.substring(1);
    }
    return name;
  }
}
