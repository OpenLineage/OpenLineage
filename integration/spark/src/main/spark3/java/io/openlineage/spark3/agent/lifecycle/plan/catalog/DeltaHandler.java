package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.agent.util.PathUtils;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.delta.catalog.DeltaCatalog;
import scala.Option;

@Slf4j
public class DeltaHandler implements CatalogHandler {
  public boolean hasClasses() {
    try {
      DeltaHandler.class
          .getClassLoader()
          .loadClass("org.apache.spark.sql.delta.catalog.DeltaCatalog");
      return true;
    } catch (Exception e) {
      // swallow- we don't care
    }
    return false;
  }

  @Override
  public boolean isClass(TableCatalog tableCatalog) {
    return tableCatalog instanceof DeltaCatalog;
  }

  @Override
  public DatasetIdentifier getDatasetIdentifier(
      SparkSession session,
      TableCatalog tableCatalog,
      Identifier identifier,
      Map<String, String> properties) {
    DeltaCatalog catalog = (DeltaCatalog) tableCatalog;

    Optional<String> location;
    if (catalog.isPathIdentifier(identifier)) {
      location = Optional.of(identifier.name());
    } else {
      location = Optional.ofNullable(properties.get("location"));
    }
    // Delta uses spark2 catalog when location isn't specified.
    Path path =
        new Path(
            location.orElse(
                session
                    .sessionState()
                    .catalog()
                    .defaultTablePath(
                        TableIdentifier.apply(
                            identifier.name(),
                            Option.apply(
                                Arrays.stream(identifier.namespace())
                                    .reduce((x, y) -> y)
                                    .orElse(null))))
                    .toString()));
    log.warn(path.toString());
    return PathUtils.fromPath(path, "file");
  }
}
