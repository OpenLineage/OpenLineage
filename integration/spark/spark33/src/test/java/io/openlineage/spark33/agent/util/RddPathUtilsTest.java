/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark33.agent.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.spark.agent.util.InputPartitionPathExtractor;
import io.openlineage.spark.agent.util.RddPathUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.execution.datasources.v2.DataSourceRDD;
import org.apache.spark.sql.execution.datasources.v2.DataSourceRDDPartition;
import org.junit.jupiter.api.Test;

class RddPathUtilsTest {

  @Test
  void testDataSourceRDDExtractor() {
    Map<InputPartition, List<Path>> partitionPaths = new HashMap<>();
    DataSourceRDD dataSourceRDD = getDataSourceRDD();

    Path expectedPath1 = new Path("/data/source1");
    DataSourceRDDPartition partition1 = getDataSourceRDDPartition(partitionPaths, expectedPath1);
    Path expectedPath2 = new Path("/data/source2");
    DataSourceRDDPartition partition2 = getDataSourceRDDPartition(partitionPaths, expectedPath2);
    when(dataSourceRDD.getPartitions())
        .thenReturn(new DataSourceRDDPartition[] {partition1, partition2});

    RddPathUtils.DataSourceRDDExtractor extractor = getDataSourceRDDExtractor(partitionPaths);

    List<Path> extractedPaths = extractor.extract(dataSourceRDD).collect(Collectors.toList());

    assertThat(extractedPaths).hasSize(2).containsExactlyInAnyOrder(expectedPath1, expectedPath2);
  }

  private static DataSourceRDD getDataSourceRDD() {
    DataSourceRDD dataSourceRDD = mock(DataSourceRDD.class);
    SparkContext sparkContext = mock(SparkContext.class);
    Configuration hadoopConfiguration = new Configuration();
    when(dataSourceRDD.sparkContext()).thenReturn(sparkContext);
    when(sparkContext.hadoopConfiguration()).thenReturn(hadoopConfiguration);
    return dataSourceRDD;
  }

  private static DataSourceRDDPartition getDataSourceRDDPartition(
      Map<InputPartition, List<Path>> partitionPaths, Path expectedPath) {
    DataSourceRDDPartition partition = mock(DataSourceRDDPartition.class);
    InputPartition inputPartition = mock(InputPartition.class);
    partitionPaths.put(inputPartition, Collections.singletonList(expectedPath));
    when(partition.inputPartitions())
        .thenReturn(ScalaConversionUtils.fromList(Collections.singletonList(inputPartition)));
    return partition;
  }

  private static RddPathUtils.DataSourceRDDExtractor getDataSourceRDDExtractor(
      Map<InputPartition, List<Path>> partitionPaths) {
    InputPartitionPathExtractor customExtractor =
        new TestInputPartitionPathExtractor(partitionPaths);

    RddPathUtils.InputPartitionPathExtractorFactory mockFactory =
        mock(RddPathUtils.InputPartitionPathExtractorFactory.class);
    when(mockFactory.createInputPartitionPathExtractors())
        .thenReturn(Collections.singletonList(customExtractor));

    return new RddPathUtils.DataSourceRDDExtractor(mockFactory);
  }

  static class TestInputPartitionPathExtractor implements InputPartitionPathExtractor {
    private final java.util.Map<InputPartition, List<Path>> partitionToPaths;

    public TestInputPartitionPathExtractor(
        java.util.Map<InputPartition, List<Path>> partitionToPaths) {
      this.partitionToPaths = partitionToPaths;
    }

    @Override
    public boolean isDefinedAt(InputPartition inputPartition) {
      return partitionToPaths.containsKey(inputPartition);
    }

    @Override
    public List<Path> extract(Configuration conf, InputPartition inputPartition) {
      return partitionToPaths.getOrDefault(inputPartition, Collections.emptyList());
    }
  }
}
