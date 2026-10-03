/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.iceberg.lifecycle.plan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
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
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ScanTask;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

class IcebergInputStatisticsInputDatasetFacetBuilderTest {

  private static final String INPUT_STATISTICS = "inputStatistics";

  @Test
  void emitsInputStatistics() {
    Map<String, InputDatasetFacet> facets = new HashMap<>();
    TestingScan scan =
        new TestingScan(
            Arrays.asList(
                task(dataFile("s3://bucket/table/data/f1.parquet", 10L, 100L)),
                task(dataFile("s3://bucket/table/data/f2.parquet", 20L, 200L))));

    builder().build(scan, facets::put);

    assertThat(facets.get(INPUT_STATISTICS))
        .isInstanceOfSatisfying(
            InputStatisticsInputDatasetFacet.class,
            facet -> {
              assertThat(facet.getFileCount()).isEqualTo(2L);
              assertThat(facet.getSize()).isEqualTo(300L);
              assertThat(facet.getRowCount()).isEqualTo(30L);
            });
  }

  @Test
  void missingFileApiDoesNotPropagate() {
    DataFile file = mock(DataFile.class, CALLS_REAL_METHODS);
    when(file.path()).thenThrow(new NoSuchMethodError("ContentFile.path()"));
    Map<String, InputDatasetFacet> facets = new HashMap<>();
    TestingScan scan = new TestingScan(Arrays.asList(task(file)));

    assertThatCode(() -> builder().build(scan, facets::put)).doesNotThrowAnyException();
    assertThat(facets).isEmpty();
  }

  private IcebergInputStatisticsInputDatasetFacetBuilder builder() {
    OpenLineageContext context = mock(OpenLineageContext.class);
    when(context.getOpenLineage()).thenReturn(new OpenLineage(URI.create("http://localhost")));
    return new IcebergInputStatisticsInputDatasetFacetBuilder(context);
  }

  private DataFile dataFile(String location, long recordCount, long sizeInBytes) {
    // calls real default methods, so ContentFile.location() resolves to path() where it exists
    DataFile file = mock(DataFile.class, CALLS_REAL_METHODS);
    when(file.path()).thenReturn(location);
    when(file.recordCount()).thenReturn(recordCount);
    when(file.fileSizeInBytes()).thenReturn(sizeInBytes);
    return file;
  }

  private ScanTask task(DataFile file) {
    FileScanTask task = mock(FileScanTask.class, CALLS_REAL_METHODS);
    when(task.file()).thenReturn(file);
    return task;
  }

  private static class TestingScan implements Scan {
    private final List<ScanTask> tasks;

    private TestingScan(List<ScanTask> tasks) {
      this.tasks = tasks;
    }

    @SuppressWarnings("unused")
    public List<ScanTask> tasks() {
      return tasks;
    }

    @Override
    public StructType readSchema() {
      return new StructType();
    }
  }
}
