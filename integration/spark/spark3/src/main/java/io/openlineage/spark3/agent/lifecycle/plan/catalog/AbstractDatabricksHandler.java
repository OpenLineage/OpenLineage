/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import scala.Option;

/**
 * The AbstractDatabricksHandler is intended to support Databricks' custom proxies which supersede
 * the open source Delta Catalog. The proprietary class name of
 * com.databricks.sql.transaction.tahoe.catalog.DeltaCatalog is used by Databricks instead of the
 * open source class name of org.apache.spark.sql.delta.catalog.DeltaCatalog. It is used in the same
 * way as the {@link DeltaHandler}.
 */
@Slf4j
public abstract class AbstractDatabricksHandler implements CatalogHandler {
  final String databricksClassNameString;

  protected final OpenLineageContext context;

  public AbstractDatabricksHandler(OpenLineageContext context) {
    this(context, "com.databricks.sql.transaction.tahoe.catalog.DeltaCatalog");
  }

  protected AbstractDatabricksHandler(OpenLineageContext context, String databricksClassName) {
    this.context = context;
    this.databricksClassNameString = databricksClassName;
  }

  @Override
  public boolean hasClasses() {
    try {
      DeltaHandler.class.getClassLoader().loadClass(databricksClassNameString);
      return true;
    } catch (NoClassDefFoundError | Exception e) {
      // If class does not exist or it's loading fails for some reason, we handle that failure by
      // returning false
    }
    return false;
  }

  @Override
  public boolean isClass(TableCatalog tableCatalog) {
    return databricksClassNameString.equals(tableCatalog.getClass().getCanonicalName());
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

    // try getting location from table properties
    if (!location.isPresent()) {
      try {
        location =
            Optional.ofNullable(tableCatalog.loadTable(identifier))
                .map(t -> t.properties())
                .filter(p -> p.containsKey("location"))
                .map(p -> p.get("location"));
      } catch (NoSuchTableException e) {
        // do nothing
      }
    }

    // get default table location
    if (!location.isPresent()) {
      location =
          Optional.ofNullable(
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
                  .toString());
    }
    Path path = new Path(location.get());
    DatasetIdentifier di = PathUtils.fromPath(path);
    return di.withSymlink(
        identifier.toString(),
        StringUtils.substringBeforeLast(
            path.toString(), File.separator), // parent location from a name becomes a namespace
        DatasetIdentifier.SymlinkType.TABLE);
  }
}
