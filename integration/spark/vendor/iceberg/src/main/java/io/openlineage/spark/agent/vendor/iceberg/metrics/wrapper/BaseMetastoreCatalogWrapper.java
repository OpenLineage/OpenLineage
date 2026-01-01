/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.iceberg.metrics.wrapper;

import io.openlineage.spark.agent.vendor.iceberg.metrics.OpenLineageMetricsReporter;
import java.lang.reflect.Field;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.metrics.MetricsReporter;

/** Wrapper to inject MetricsReporter into RESTCatalog. Uses reflection to access private fields. */
@Slf4j
public class BaseMetastoreCatalogWrapper implements CatalogWrapper {

  public static final String METRICS_REPORTER = "metricsReporter";
  BaseMetastoreCatalog catalog;

  public BaseMetastoreCatalogWrapper(BaseMetastoreCatalog catalog) {
    this.catalog = catalog;
  }

  /**
   * Get the metricsReporter field from {@link BaseMetastoreCatalog}. Returns null if the field is
   * not found.
   *
   * @return MetricsReporter
   */
  @Override
  public MetricsReporter getExistingReporter() {
    Field metricsReporterField =
        FieldUtils.getField(BaseMetastoreCatalog.class, METRICS_REPORTER, true);

    if (metricsReporterField == null) {
      log.warn("Could obtain metrics reporter: no such field");
      return null;
    }

    try {
      return (MetricsReporter) metricsReporterField.get(catalog);
    } catch (IllegalAccessException e) {
      log.warn("Could obtain metrics reporter: {}", e.getMessage());
      return null;
    }
  }

  /**
   * Update the metricsReporter field. Throws IllegalAccessException if the field is not found.
   *
   * @param reporter OpenLineageMetricsReporter
   */
  @Override
  public void updateMetricsReporter(OpenLineageMetricsReporter reporter)
      throws IllegalAccessException {
    Field reporterField = FieldUtils.getField(BaseMetastoreCatalog.class, METRICS_REPORTER, true);
    reporterField.set(catalog, reporter);
  }
}
