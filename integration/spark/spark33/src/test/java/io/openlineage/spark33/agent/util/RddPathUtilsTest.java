/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark33.agent.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.util.InputPartitionExtractor;
import io.openlineage.spark.agent.util.PathUtils;
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
    Map<InputPartition, List<DatasetIdentifier>> datasetIdentifiers = new HashMap<>();
    DataSourceRDD dataSourceRDD = getDataSourceRDD();

    DataSourceRDDPartition partition1 =
        getDataSourceRDDPartition(
            datasetIdentifiers, PathUtils.fromPath(new Path("/warehouse/source1")));
    DataSourceRDDPartition partition2 =
        getDataSourceRDDPartition(
            datasetIdentifiers,
            PathUtils.fromPath(new Path("/warehouse/source2"))
                .withSymlink(
                    "default.source2", "file:/warehouse", DatasetIdentifier.SymlinkType.TABLE));
    when(dataSourceRDD.getPartitions())
        .thenReturn(new DataSourceRDDPartition[] {partition1, partition2});

    RddPathUtils.DataSourceRDDExtractor extractor = getDataSourceRDDExtractor(datasetIdentifiers);

    List<DatasetIdentifier> extracted =
        extractor.extract(dataSourceRDD).collect(Collectors.toList());

    assertThat(extracted)
        .extracting(
            DatasetIdentifier::getNamespace,
            DatasetIdentifier::getName,
            DatasetIdentifier::getSymlinks)
        .containsExactlyInAnyOrder(
            tuple("file", "/warehouse/source1", Collections.emptyList()),
            tuple(
                "file",
                "/warehouse/source2",
                Collections.singletonList(
                    new DatasetIdentifier.Symlink(
                        "default.source2",
                        "file:/warehouse",
                        DatasetIdentifier.SymlinkType.TABLE))));
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
      Map<InputPartition, List<DatasetIdentifier>> partitionToDatasetIdentifiers,
      DatasetIdentifier expectedDatasetIdentifier) {
    DataSourceRDDPartition partition = mock(DataSourceRDDPartition.class);
    InputPartition inputPartition = mock(InputPartition.class);
    partitionToDatasetIdentifiers.put(
        inputPartition, Collections.singletonList(expectedDatasetIdentifier));
    when(partition.inputPartitions())
        .thenReturn(ScalaConversionUtils.fromList(Collections.singletonList(inputPartition)));
    return partition;
  }

  private static RddPathUtils.DataSourceRDDExtractor getDataSourceRDDExtractor(
      Map<InputPartition, List<DatasetIdentifier>> partitionToDatasetIdentifiers) {
    InputPartitionExtractor customExtractor =
        new TestInputPartitionExtractor(partitionToDatasetIdentifiers);

    RddPathUtils.InputPartitionExtractorFactory mockFactory =
        mock(RddPathUtils.InputPartitionExtractorFactory.class);
    when(mockFactory.createInputPartitionExtractors())
        .thenReturn(Collections.singletonList(customExtractor));

    return new RddPathUtils.DataSourceRDDExtractor(mockFactory);
  }

  static class TestInputPartitionExtractor implements InputPartitionExtractor {
    private final java.util.Map<InputPartition, List<DatasetIdentifier>>
        partitionToDatasetIdentifiers;

    public TestInputPartitionExtractor(
        java.util.Map<InputPartition, List<DatasetIdentifier>> partitionToDatasetIdentifiers) {
      this.partitionToDatasetIdentifiers = partitionToDatasetIdentifiers;
    }

    @Override
    public boolean isDefinedAt(InputPartition inputPartition) {
      return partitionToDatasetIdentifiers.containsKey(inputPartition);
    }

    @Override
    public List<DatasetIdentifier> extract(
        SparkContext sparkContext, InputPartition inputPartition) {
      return partitionToDatasetIdentifiers.getOrDefault(inputPartition, Collections.emptyList());
    }
  }
}
