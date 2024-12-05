/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.spark.rdd.MapPartitionsRDD;
import org.apache.spark.rdd.ParallelCollectionRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.execution.datasources.FilePartition;
import org.apache.spark.sql.execution.datasources.FileScanRDD;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.junit.jupiter.api.Test;
import scala.collection.JavaConverters;
import scala.collection.Seq;

class RddPathUtilsTest {

  @Test
  void testFindRDDPathsForMapPartitionsRDD() {
    FilePartition filePartition = mock(FilePartition.class);
    PartitionedFile partitionedFile = mock(PartitionedFile.class);
    FileScanRDD fileScanRDD = mock(FileScanRDD.class);
    MapPartitionsRDD mapPartitions = mock(MapPartitionsRDD.class);

    when(mapPartitions.prev()).thenReturn(fileScanRDD);
    when(fileScanRDD.filePartitions())
        .thenReturn(ScalaConversionUtils.fromList(Collections.singletonList(filePartition)));
    when(filePartition.files()).thenReturn(new PartitionedFile[] {partitionedFile});
    when(partitionedFile.filePath()).thenReturn("/some-path/sub-path");

    List rddPaths = PlanUtils.findRDDPaths(Collections.singletonList(mapPartitions));

    assertThat(rddPaths).hasSize(1);
    assertThat(rddPaths.get(0).toString()).isEqualTo("/some-path");
  }

  @Test
  void testFindRDDPathsForParallelCollectionRDDWhenNoDataField() throws IllegalAccessException {
    ParallelCollectionRDD parallelCollectionRDD = mock(ParallelCollectionRDD.class);

    FieldUtils.writeDeclaredField(parallelCollectionRDD, "data", null, true);
    assertThat(PlanUtils.findRDDPaths(Collections.singletonList(parallelCollectionRDD))).hasSize(0);
  }

  @Test
  void testFindRDDPathsForParallelCollectionRDDWhenDataFieldNotSeqOfTuples()
      throws IllegalAccessException {
    ParallelCollectionRDD parallelCollectionRDD = mock(ParallelCollectionRDD.class);
    Seq<Integer> data =
        JavaConverters.asScalaIteratorConverter(Arrays.asList(333).iterator()).asScala().toSeq();

    FieldUtils.writeDeclaredField(parallelCollectionRDD, "data", data, true);
    assertThat(PlanUtils.findRDDPaths(Collections.singletonList(parallelCollectionRDD))).hasSize(0);
  }

  @Test
  void testFindRDDPathsEmptyStringPath() {
    FilePartition filePartition = mock(FilePartition.class);
    FileScanRDD fileScanRDD = mock(FileScanRDD.class);
    PartitionedFile partitionedFile = mock(PartitionedFile.class);

    when(filePartition.files()).thenReturn(new PartitionedFile[] {partitionedFile});
    when(partitionedFile.filePath()).thenReturn("");
    when(fileScanRDD.filePartitions())
        .thenReturn(ScalaConversionUtils.fromList(Collections.singletonList(filePartition)));

    List rddPaths = PlanUtils.findRDDPaths(Collections.singletonList(fileScanRDD));

    assertThat(rddPaths).hasSize(0);
  }

  @Test
  void testFindRDDPathsUnknownRdd() {
    assertThat(PlanUtils.findRDDPaths(Collections.singletonList(mock(RDD.class)))).isEmpty();
  }
}
