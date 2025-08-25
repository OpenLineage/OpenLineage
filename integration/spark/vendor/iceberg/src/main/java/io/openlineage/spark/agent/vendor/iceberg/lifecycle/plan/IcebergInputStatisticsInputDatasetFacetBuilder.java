/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.iceberg.lifecycle.plan;

import io.openlineage.client.OpenLineage.InputDatasetFacet;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ScanTask;
import org.apache.spark.sql.connector.read.Scan;

/**
 * Extracts input statistics from Iceberg scan tasks.
 *
 * <p>Extracts the number of files, the total size of the files, and the total number of records
 * from the Iceberg scan tasks.
 *
 * <p>Behavior is tested in:
 * io.openlineage.spark.agent.SparkIcebergIntegrationTest#sparkEmitsInputAndOutputStatistics()
 */
@Slf4j
public class IcebergInputStatisticsInputDatasetFacetBuilder
    extends CustomFacetBuilder<Scan, InputDatasetFacet> {

  private final OpenLineageContext context;

  public IcebergInputStatisticsInputDatasetFacetBuilder(OpenLineageContext context) {
    super();
    this.context = context;
  }

  @Override
  public boolean isDefinedAt(Object x) {
    if (!(x instanceof Scan)) {
      return false;
    }

    // should be defined for `org.apache.iceberg.spark.source.SparkBatchQueryScan` which is not
    // public class
    return x.getClass().getCanonicalName().startsWith("org.apache.iceberg.spark");
  }

  @Override
  protected void build(Scan scan, BiConsumer<String, ? super InputDatasetFacet> consumer) {
    if (scan == null) {
      return;
    }
    try {
      List<ScanTask> tasks = (List<ScanTask>) MethodUtils.invokeMethod(scan, true, "tasks");

      Collection<DataFile> dataFiles =
          tasks.stream()
              .flatMap(
                  task -> {
                    if (task instanceof BaseCombinedScanTask) {
                      return ((BaseCombinedScanTask) task).files().stream();
                    }
                    return Stream.of(task);
                  })
              .filter(ScanTask::isFileScanTask)
              .map(ScanTask::asFileScanTask)
              .map(FileScanTask::file)
              .collect(Collectors.toMap(ContentFile::path, f -> f))
              .values();

      consumer.accept(
          "inputStatistics",
          context
              .getOpenLineage()
              .newInputStatisticsInputDatasetFacetBuilder()
              .fileCount((long) dataFiles.size())
              .size(dataFiles.stream().map(ContentFile::fileSizeInBytes).reduce(0L, Long::sum))
              .rowCount(dataFiles.stream().map(ContentFile::recordCount).reduce(0L, Long::sum))
              .build());
    } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
      // do nothing
      log.warn(
          "Failed to extract input statistics from Iceberg scan class {}",
          scan.getClass().getCanonicalName());
      log.debug("Failed to extract input statistics from Iceberg scan", e);
    }
  }
}
