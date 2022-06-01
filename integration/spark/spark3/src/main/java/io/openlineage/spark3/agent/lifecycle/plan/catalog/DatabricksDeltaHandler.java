package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import io.openlineage.spark.shared.agent.facets.TableProviderFacet;
import io.openlineage.spark.shared.agent.util.DatasetIdentifier;
import io.openlineage.spark.shared.agent.util.PathUtils;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import scala.Option;

/**
 * The DatabricksDeltaHandler is intended to support Databricks' custom DeltaCatalog which has the
 * class name of com.databricks.sql.transaction.tahoe.catalog.DeltaCatalog rather than the open
 * source class name of org.apache.spark.sql.delta.catalog.DeltaCatalog. It is used in the same way
 * as the {@link DeltaHandler}.
 */
@Slf4j
public class DatabricksDeltaHandler implements CatalogHandler {
  public boolean hasClasses() {
    try {
      DeltaHandler.class
          .getClassLoader()
          .loadClass("com.databricks.sql.transaction.tahoe.catalog.DeltaCatalog");
      return true;
    } catch (Exception e) {
      // swallow- we don't care
    }
    return false;
  }

  @Override
  public boolean isClass(TableCatalog tableCatalog) {
    return tableCatalog
        .getClass()
        .getCanonicalName()
        .equals("com.databricks.sql.transaction.tahoe.catalog.DeltaCatalog");
  }

  @Override
  public DatasetIdentifier getDatasetIdentifier(
      SparkSession session,
      TableCatalog tableCatalog,
      Identifier identifier,
      Map<String, String> properties) {

    Optional<String> location;
    boolean isPathIdentifier = false;
    try {
      isPathIdentifier =
          (boolean) MethodUtils.invokeMethod(tableCatalog, true, "isPathIdentifier", identifier);
    } catch (InvocationTargetException | IllegalAccessException | NoSuchMethodException e) {
      // DO Nothing
    }

    if (isPathIdentifier) {
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
    log.info(path.toString());
    return PathUtils.fromPath(path, "file");
  }

  public Optional<TableProviderFacet> getTableProviderFacet(Map<String, String> properties) {
    return Optional.of(new TableProviderFacet("delta", "parquet")); // Delta is always parquet
  }

  @Override
  public String getName() {
    return "delta";
  }
}
