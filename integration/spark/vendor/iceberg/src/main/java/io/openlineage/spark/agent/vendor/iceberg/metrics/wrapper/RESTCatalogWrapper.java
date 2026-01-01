/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.iceberg.metrics.wrapper;

import io.openlineage.spark.agent.vendor.iceberg.metrics.OpenLineageMetricsReporter;
import java.lang.reflect.Field;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTSessionCatalog;

/** Wrapper to inject MetricsReporter into RESTCatalog. Uses reflection to access private fields. */
@Slf4j
public class RESTCatalogWrapper implements CatalogWrapper {

  public static final String REPORTER = "reporter";
  RESTCatalog catalog;
  RESTSessionCatalog restSessionCatalog;

  public RESTCatalogWrapper(RESTCatalog catalog) {
    this.catalog = catalog;
    loadRestSessionCatalog();
  }

  private void loadRestSessionCatalog() {
    try {
      Field sessionCatalogField = FieldUtils.getField(RESTCatalog.class, "sessionCatalog", true);
      restSessionCatalog = (RESTSessionCatalog) sessionCatalogField.get(catalog);
    } catch (IllegalAccessException | ClassCastException e) {
      log.warn("Unable to inject metrics reporter to RESTCatalog {}", e.getMessage());
    }
  }

  /**
   * Get the reporter field from {@link RESTSessionCatalog} within {@link RESTCatalog}. Returns null
   * if the field is not found.
   *
   * @return MetricsReporter
   */
  public MetricsReporter getExistingReporter() {
    Field reporterField = FieldUtils.getField(RESTSessionCatalog.class, REPORTER, true);
    if (reporterField == null) {
      log.warn("Could obtain metrics reporter: no such field");
      return null;
    }
    try {
      return (MetricsReporter) reporterField.get(restSessionCatalog);
    } catch (IllegalAccessException e) {
      log.warn("Could obtain metrics reporter: {}", e.getMessage());
      return null;
    }
  }

  /**
   * Update the reporter field of {@link RESTSessionCatalog} within {@link RESTCatalog}. Throws
   * IllegalAccessException if the field is not found.
   *
   * @param reporter OpenLineageMetricsReporter
   */
  @Override
  public void updateMetricsReporter(OpenLineageMetricsReporter reporter)
      throws IllegalAccessException {
    Field reporterField = FieldUtils.getField(RESTSessionCatalog.class, REPORTER, true);
    reporterField.set(restSessionCatalog, reporter);
  }
}
