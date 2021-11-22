package io.openlineage.spark.agent.util;

import java.net.URI;
import java.util.Optional;
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
    log.warn(path.toString());
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
    log.warn(namespace);
    String name = fixName(uri.getPath());
    log.warn(name);
    return new DatasetIdentifier(name, namespace);
  }

  public static DatasetIdentifier fromURI(URI location, String defaultScheme) {
    return fromPath(new Path(location), defaultScheme);
  }

  public static DatasetIdentifier fromCatalogTable(CatalogTable catalogTable, String authority) {
    try {
      URI location = catalogTable.location();
      return PathUtils.fromURI(location, "file");
    } catch (Exception e) { // Java does not recognize scala exception
      if (e instanceof AnalysisException) {
        return PathUtils.fromPath(new Path("hive", authority, catalogTable.qualifiedName()));
      }
      throw e;
    }
  }

  public static DatasetIdentifier fromCatalogTable(CatalogTable catalogTable) {
    String authority =
        ScalaConversionUtils.asJavaOptional(catalogTable.storage().locationUri())
            .map(URI::toString)
            .orElse(
                SparkSession.active()
                    .sessionState()
                    .catalog()
                    .defaultTablePath(catalogTable.identifier())
                    .toString());
    return PathUtils.fromCatalogTable(catalogTable, authority);
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
