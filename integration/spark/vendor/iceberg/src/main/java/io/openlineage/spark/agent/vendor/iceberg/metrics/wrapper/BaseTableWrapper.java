/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.iceberg.metrics.wrapper;

import io.openlineage.spark.agent.vendor.iceberg.metrics.OpenLineageMetricsReporter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.metrics.MetricsReporter;

/**
 * Wrapper to attach an {@link OpenLineageMetricsReporter} to an Iceberg {@link BaseTable}.
 *
 * <p>Iceberg copies the catalog's metrics reporter into each table object when the object is
 * created, and commits report to the table's copy. Tables loaded before the reporter was injected
 * into their catalog, for example during analysis of the current query or kept by {@code
 * CachingCatalog}, therefore never report to OpenLineage unless the reporter is attached to the
 * table object as well. Attachment requires Iceberg 1.11's public methods, called reflectively to
 * keep the module compatible with older runtimes.
 */
@Slf4j
public final class BaseTableWrapper {

  private BaseTableWrapper() {}

  /**
   * Makes the table report to the given OpenLineage reporter in addition to the reporters it
   * already uses. Does nothing if the table already reports to it. Never throws: if the reporter
   * cannot be attached, the failure is logged at debug level.
   *
   * @param table Iceberg table
   * @param reporter reporter registered for the table's catalog
   */
  public static void attach(Table table, OpenLineageMetricsReporter reporter) {
    if (!(table instanceof BaseTable)) {
      return;
    }

    try {
      Method combine =
          MethodUtils.getAccessibleMethod(
              BaseTable.class, "combineMetricsReporter", MetricsReporter.class);
      if (combine == null) {
        return;
      }
      // Tables created after catalog injection already use the full reporter.
      if (MethodUtils.invokeMethod(table, "reporter") != reporter) {
        // Iceberg deduplicates repeated combinations of the same reporter instance.
        combine.invoke(table, reporter.getTableReporter());
      }
    } catch (NoSuchMethodException
        | IllegalAccessException
        | InvocationTargetException
        | RuntimeException
        | LinkageError e) {
      log.debug("Unable to attach metrics reporter to Iceberg table {}", table.name(), e);
    }
  }
}
