/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.iceberg.metrics.wrapper;

import io.openlineage.spark.agent.vendor.iceberg.metrics.OpenLineageMetricsReporter;
import org.apache.iceberg.metrics.MetricsReporter;

public interface CatalogWrapper {

  /**
   * Get the existing metrics reporter from the catalog. Returns null if the field is not found.
   *
   * @return MetricsReporter
   */
  MetricsReporter getExistingReporter();

  /**
   * Update the metrics reporter in the catalog. Throws IllegalAccessException if the field is not.
   *
   * @param reporter OpenLineageMetricsReporter
   */
  void updateMetricsReporter(OpenLineageMetricsReporter reporter) throws IllegalAccessException;
}
