/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.spark.Partition;
import org.apache.spark.rdd.MapPartitionsRDD;
import org.apache.spark.rdd.ParallelCollectionRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.execution.datasources.FilePartition;
import org.apache.spark.sql.execution.datasources.FileScanRDD;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.junit.jupiter.api.Test;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;

public class RddPathsUtilsTest {

  @Test
  void testFindRDDPathsForMapPartitionsRDD() {
    FilePartition filePartition = mock(FilePartition.class);
    PartitionedFile partitionedFile = mock(PartitionedFile.class);
    FileScanRDD fileScanRDD = mock(FileScanRDD.class);
    MapPartitionsRDD mapPartitions = mock(MapPartitionsRDD.class);

    when(mapPartitions.prev()).thenReturn(fileScanRDD);
    when(fileScanRDD.filePartitions())
        .thenReturn(JavaConversions.asScalaBuffer(Collections.singletonList(filePartition)));
    when(filePartition.files()).thenReturn(new PartitionedFile[] {partitionedFile});
    when(partitionedFile.filePath()).thenReturn("/some-path/sub-path");

    List rddPaths = PlanUtils.findRDDPaths(Collections.singletonList(mapPartitions));

    assertThat(rddPaths).hasSize(1);
    assertThat(rddPaths.get(0).toString()).isEqualTo("/some-path");
  }

  @Test
  void testFindRDDPathsForParallelCollectionRDD() {
    ParallelCollectionRDD parallelCollectionRDD = mock(ParallelCollectionRDD.class);
    Partition partition = mock(Partition.class);
    when(parallelCollectionRDD.getPartitions()).thenReturn(new Partition[] {partition});
    when(parallelCollectionRDD.getPreferredLocations(partition))
        .thenReturn(
            JavaConverters.asScalaIteratorConverter(
                    Arrays.asList("/some-path1/sub-path", "/some-path2/sub-path", "").iterator())
                .asScala()
                .toSeq());

    List rddPaths = PlanUtils.findRDDPaths(Collections.singletonList(parallelCollectionRDD));

    assertThat(rddPaths).hasSize(2);
    assertThat(rddPaths.get(0).toString()).isEqualTo("/some-path1");
    assertThat(rddPaths.get(1).toString()).isEqualTo("/some-path2");
  }

  @Test
  void testFindRDDPathsEmptyStringPath() {
    FilePartition filePartition = mock(FilePartition.class);
    FileScanRDD fileScanRDD = mock(FileScanRDD.class);
    PartitionedFile partitionedFile = mock(PartitionedFile.class);

    when(filePartition.files()).thenReturn(new PartitionedFile[] {partitionedFile});
    when(partitionedFile.filePath()).thenReturn("");
    when(fileScanRDD.filePartitions())
        .thenReturn(JavaConversions.asScalaBuffer(Collections.singletonList(filePartition)));

    List rddPaths = PlanUtils.findRDDPaths(Collections.singletonList(fileScanRDD));

    assertThat(rddPaths).hasSize(0);
  }

  @Test
  void testFindRDDPathsUnknownRdd() {
    assertThat(PlanUtils.findRDDPaths(Collections.singletonList(mock(RDD.class)))).isEmpty();
  }
}
