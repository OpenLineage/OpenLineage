/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.iceberg.lifecycle.plan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDatasetFacet;
import io.openlineage.client.OpenLineage.InputStatisticsInputDatasetFacet;
import io.openlineage.spark.api.OpenLineageContext;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ScanTask;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

class IcebergInputStatisticsInputDatasetFacetBuilderTest {

  private static final String INPUT_STATISTICS = "inputStatistics";
  private static final String FILE_1 = "s3://bucket/table/data/file-1.parquet";
  private static final String FILE_2 = "s3://bucket/table/data/file-2.parquet";

  @Test
  void deduplicatesTasksReadingTheSameFile() {
    DataFile file1 = dataFile(FILE_1, 100L, 10L);
    DataFile file2 = dataFile(FILE_2, 200L, 20L);

    InputStatisticsInputDatasetFacet facet =
        buildFacet(
            new TestingScan(
                Arrays.asList(fileScanTask(file1), fileScanTask(file1), fileScanTask(file2))));

    assertThat(facet.getFileCount()).isEqualTo(2L);
    assertThat(facet.getSize()).isEqualTo(300L);
    assertThat(facet.getRowCount()).isEqualTo(30L);
  }

  @Test
  void deduplicatesSplitsOfTheSameFileInCombinedScanTask() {
    DataFile file = dataFile(FILE_1, 100L, 10L);
    ScanTask combinedTask = new BaseCombinedScanTask(fileScanTask(file), fileScanTask(file));

    InputStatisticsInputDatasetFacet facet =
        buildFacet(new TestingScan(Arrays.asList(combinedTask)));

    assertThat(facet.getFileCount()).isEqualTo(1L);
    assertThat(facet.getSize()).isEqualTo(100L);
    assertThat(facet.getRowCount()).isEqualTo(10L);
  }

  private InputStatisticsInputDatasetFacet buildFacet(Scan scan) {
    OpenLineageContext context = mock(OpenLineageContext.class);
    when(context.getOpenLineage()).thenReturn(new OpenLineage(URI.create("http://test")));
    Map<String, InputDatasetFacet> facets = new HashMap<>();

    new IcebergInputStatisticsInputDatasetFacetBuilder(context).build(scan, facets::put);

    assertThat(facets).containsKey(INPUT_STATISTICS);
    return (InputStatisticsInputDatasetFacet) facets.get(INPUT_STATISTICS);
  }

  private static DataFile dataFile(String path, long size, long records) {
    DataFile file = mock(DataFile.class);
    when(file.path()).thenReturn(path);
    when(file.fileSizeInBytes()).thenReturn(size);
    when(file.recordCount()).thenReturn(records);
    return file;
  }

  private static FileScanTask fileScanTask(DataFile file) {
    FileScanTask task = mock(FileScanTask.class);
    when(task.isFileScanTask()).thenReturn(true);
    when(task.asFileScanTask()).thenReturn(task);
    when(task.file()).thenReturn(file);
    return task;
  }

  private static class TestingScan implements Scan {
    private final List<ScanTask> tasks;

    private TestingScan(List<ScanTask> tasks) {
      this.tasks = tasks;
    }

    public List<ScanTask> tasks() {
      return tasks;
    }

    @Override
    public StructType readSchema() {
      return new StructType();
    }
  }
}
