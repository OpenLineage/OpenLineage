/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg;

import java.lang.reflect.Method;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.source.SparkTable;

/**
 * Unwraps the Iceberg {@link Table} from the Spark connector tables Iceberg hands to Spark.
 *
 * <p>{@link SparkTable} is the common case, but it is not the only wrapper. From Iceberg 1.11 on,
 * Iceberg's Spark 4.1 module also ships {@code SparkRewriteTable}, which its rewrite actions read
 * and write through. It extends the package-private {@code BaseSparkTable} rather than {@link
 * SparkTable} and is absent from the Iceberg version this module compiles against, so it - and any
 * similar wrapper - is unwrapped reflectively through its public {@code table()} accessor.
 */
@Slf4j
final class IcebergSparkTables {
  /** The package of Iceberg's Spark connector tables - {@code SparkTable} and its siblings. */
  static final String ICEBERG_SPARK_SOURCE_PACKAGE = "org.apache.iceberg.spark.source.";

  private static final String TABLE_ACCESSOR = "table";

  private IcebergSparkTables() {}

  /**
   * Returns the Iceberg table behind a Spark table that belongs to Iceberg's Spark connector, or
   * empty for any other table. Tables from other packages are rejected without reflection, so a
   * foreign table that happens to expose a {@code table()} method is never mistaken for Iceberg.
   */
  static Optional<Table> fromIcebergSparkTable(
      org.apache.spark.sql.connector.catalog.Table sparkTable) {
    if (sparkTable instanceof SparkTable) {
      return Optional.ofNullable(((SparkTable) sparkTable).table());
    }
    if (sparkTable == null
        || !sparkTable.getClass().getName().startsWith(ICEBERG_SPARK_SOURCE_PACKAGE)) {
      return Optional.empty();
    }
    return fromTableAccessor(sparkTable);
  }

  /**
   * Returns the Iceberg table returned by the Spark table's public {@code table()} method, or empty
   * when there is no such method or it does not return an Iceberg table.
   */
  static Optional<Table> fromTableAccessor(
      org.apache.spark.sql.connector.catalog.Table sparkTable) {
    if (sparkTable == null) {
      return Optional.empty();
    }
    String tableClass = sparkTable.getClass().getName();
    try {
      Method accessor = sparkTable.getClass().getMethod(TABLE_ACCESSOR);
      Object result = accessor.invoke(sparkTable);
      if (result instanceof Table) {
        return Optional.of((Table) result);
      } else if (result != null) {
        log.warn(
            "table() method returned non-Table type: {} for table class: {}",
            result.getClass().getName(),
            tableClass);
      }
    } catch (NoSuchMethodException e) {
      log.debug(
          "No table() method found on table type: {}. This may not be an Iceberg table wrapper.",
          tableClass);
    } catch (ReflectiveOperationException | RuntimeException | LinkageError e) {
      log.warn("Failed to extract Iceberg Table via reflection from table type: {}", tableClass, e);
    }
    return Optional.empty();
  }
}
