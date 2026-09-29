/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.iceberg.metrics.wrapper;

import io.openlineage.spark.agent.vendor.iceberg.metrics.OpenLineageMetricsReporter;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.metrics.MetricsReporters;

/**
 * Wrapper to attach an {@link OpenLineageMetricsReporter} to an Iceberg {@link BaseTable}.
 *
 * <p>Iceberg copies the catalog's metrics reporter into each table object when the object is
 * created, and commits report to the table's copy. Tables loaded before the reporter was injected
 * into their catalog, for example during analysis of the current query or kept by {@code
 * CachingCatalog}, therefore never report to OpenLineage unless the reporter is attached to the
 * table object as well. Uses reflection because Iceberg releases before 1.11 have no public API to
 * change the reporter of a table.
 */
@Slf4j
public final class BaseTableWrapper {

  public static final String REPORTER = "reporter";
  public static final String COMBINE_METRICS_REPORTER = "combineMetricsReporter";
  private static final String COMPOSITE_REPORTERS = "reporters";

  private BaseTableWrapper() {}

  /**
   * Makes the table report to the given OpenLineage reporter in addition to the reporters it
   * already uses. Does nothing if the table already reports to it. Never throws: if the reporter
   * cannot be attached, the failure is logged at debug level.
   *
   * @param table Iceberg table
   * @param reporter reporter registered for the table's catalog
   * @return true if the table reports to the reporter after the call
   */
  public static boolean attach(Table table, OpenLineageMetricsReporter reporter) {
    if (!(table instanceof BaseTable)) {
      log.debug("Cannot attach metrics reporter to table of type {}", table.getClass().getName());
      return false;
    }

    try {
      Field reporterField = FieldUtils.getField(BaseTable.class, REPORTER, true);
      if (reporterField == null) {
        log.debug("Cannot attach metrics reporter to table {}: no reporter field", table.name());
        return false;
      }

      MetricsReporter current = (MetricsReporter) reporterField.get(table);
      MetricsReporter tableReporter = reporter.getTableReporter();
      if (reportsTo(current, reporter) || reportsTo(current, tableReporter)) {
        return true;
      }

      Method combine =
          MethodUtils.getAccessibleMethod(
              table.getClass(), COMBINE_METRICS_REPORTER, MetricsReporter.class);
      if (combine != null) {
        // public API since Iceberg 1.11
        combine.invoke(table, tableReporter);
      } else {
        // the field is final before Iceberg 1.11
        reporterField.set(table, MetricsReporters.combine(current, tableReporter));
      }
      log.debug("Attached metrics reporter to Iceberg table {}", table.name());
      return true;
    } catch (ReflectiveOperationException | RuntimeException | LinkageError e) {
      log.debug("Unable to attach metrics reporter to Iceberg table {}", table.name(), e);
      return false;
    }
  }

  private static boolean reportsTo(MetricsReporter current, MetricsReporter reporter)
      throws IllegalAccessException {
    if (current == reporter) {
      return true;
    }
    if (current == null) {
      return false;
    }

    // MetricsReporters.combine returns a composite reporter that keeps the combined reporters
    Field reportersField = FieldUtils.getField(current.getClass(), COMPOSITE_REPORTERS, true);
    if (reportersField == null) {
      return false;
    }
    Object reporters = reportersField.get(current);
    if (!(reporters instanceof Collection)) {
      return false;
    }
    for (Object combined : (Collection<?>) reporters) {
      if (combined == reporter) {
        return true;
      }
    }
    return false;
  }
}
