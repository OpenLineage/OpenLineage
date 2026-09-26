/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package org.apache.iceberg.spark;

import java.util.function.Supplier;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;

public class TestingScanWithReport implements Scan {
  private final Supplier<ScanReport> scanReportSupplier;

  public TestingScanWithReport(Supplier<ScanReport> scanReportSupplier) {
    this.scanReportSupplier = scanReportSupplier;
  }

  @Override
  public StructType readSchema() {
    return new StructType();
  }
}
