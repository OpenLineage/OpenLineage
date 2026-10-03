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
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.spark.TestingScanWithTasks;
import org.junit.jupiter.api.Test;

class IcebergInputStatisticsInputDatasetFacetBuilderTest {

  private static final String INPUT_STATISTICS = "inputStatistics";

  @Test
  void emitsInputStatisticsWhenFacetEnabled() {
    TestingScanWithTasks scan = new TestingScanWithTasks(Collections.singletonList(fileScanTask()));
    Map<String, InputDatasetFacet> facets = new HashMap<>();

    IcebergInputStatisticsInputDatasetFacetBuilder builder = builder(false);
    builder.accept(scan, facets::put);

    assertThat(builder.isDefinedAt(scan)).isTrue();
    assertThat(facets.get(INPUT_STATISTICS))
        .isInstanceOfSatisfying(
            InputStatisticsInputDatasetFacet.class,
            facet -> {
              assertThat(facet.getFileCount()).isEqualTo(1L);
              assertThat(facet.getSize()).isEqualTo(100L);
              assertThat(facet.getRowCount()).isEqualTo(10L);
            });
  }

  @Test
  void doesNotReadScanTasksWhenFacetDisabled() {
    TestingScanWithTasks scan = new TestingScanWithTasks(Collections.singletonList(fileScanTask()));
    Map<String, InputDatasetFacet> facets = new HashMap<>();

    IcebergInputStatisticsInputDatasetFacetBuilder builder = builder(true);
    builder.accept(scan, facets::put);

    assertThat(builder.isDefinedAt(scan)).isFalse();
    assertThat(scan.getTasksCalls()).isZero();
    assertThat(facets).isEmpty();
  }

  private IcebergInputStatisticsInputDatasetFacetBuilder builder(boolean statisticsDisabled) {
    SparkOpenLineageConfig config = new SparkOpenLineageConfig();
    config
        .getFacetsConfig()
        .setDisabledFacets(Collections.singletonMap(INPUT_STATISTICS, statisticsDisabled));
    OpenLineageContext context = mock(OpenLineageContext.class);
    when(context.getOpenLineageConfig()).thenReturn(config);
    when(context.getOpenLineage()).thenReturn(new OpenLineage(URI.create("https://test")));
    return new IcebergInputStatisticsInputDatasetFacetBuilder(context);
  }

  private FileScanTask fileScanTask() {
    DataFile file = mock(DataFile.class);
    when(file.path()).thenReturn("s3://bucket/table/data/file.parquet");
    when(file.fileSizeInBytes()).thenReturn(100L);
    when(file.recordCount()).thenReturn(10L);

    FileScanTask task = mock(FileScanTask.class);
    when(task.isFileScanTask()).thenReturn(true);
    when(task.asFileScanTask()).thenReturn(task);
    when(task.file()).thenReturn(file);
    return task;
  }
}
