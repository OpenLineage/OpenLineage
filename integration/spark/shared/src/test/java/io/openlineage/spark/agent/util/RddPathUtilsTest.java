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
import org.apache.spark.rdd.MapPartitionsRDD;
import org.apache.spark.rdd.ParallelCollectionRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.execution.datasources.FilePartition;
import org.apache.spark.sql.execution.datasources.FileScanRDD;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.apache.commons.lang3.reflect.FieldUtils;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;

public class RddPathUtilsTest {

  @Test
  void testFindRDDPathsForMapPartitionsRDD() {
    FilePartition filePartition = mock(FilePartition.class);
    PartitionedFile partitionedFile = mock(PartitionedFile.class);
    FileScanRDD fileScanRDD = mock(FileScanRDD.class);
    MapPartitionsRDD mapPartitions = mock(MapPartitionsRDD.class);

    when(mapPartitions.prev()).thenReturn(fileScanRDD);
    when(fileScanRDD.filePartitions())
        .thenReturn(ScalaConversionUtils.asScalaSeq(Collections.singletonList(filePartition)));
    when(filePartition.files()).thenReturn(new PartitionedFile[] {partitionedFile});
    when(partitionedFile.filePath()).thenReturn("/some-path/sub-path");

    List rddPaths = PlanUtils.findRDDPaths(Collections.singletonList(mapPartitions));

    assertThat(rddPaths).hasSize(1);
    assertThat(rddPaths.get(0).toString()).isEqualTo("/some-path");
  }

  @Test
  void testFindRDDPathsForParallelCollectionRDD() throws IllegalAccessException {
    ParallelCollectionRDD parallelCollectionRDD = mock(ParallelCollectionRDD.class);
    Seq<Tuple2<String, Integer>> data =
        JavaConverters.asScalaIteratorConverter(
                Arrays.asList(
                        new Tuple2<>("/some-path1/data-file-325342.snappy.parquet", 345),
                        new Tuple2<>("/some-path2/data-file-654342.snappy.parquet", 345))
                    .iterator())
            .asScala()
            .toSeq();

    FieldUtils.writeDeclaredField(parallelCollectionRDD, "data", data, true);

    List rddPaths = PlanUtils.findRDDPaths(Collections.singletonList(parallelCollectionRDD));

    assertThat(rddPaths).hasSize(2);
    assertThat(rddPaths.get(0).toString()).isEqualTo("/some-path1");
    assertThat(rddPaths.get(1).toString()).isEqualTo("/some-path2");
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
        .thenReturn(ScalaConversionUtils.asScalaSeq(Collections.singletonList(filePartition)));

    List rddPaths = PlanUtils.findRDDPaths(Collections.singletonList(fileScanRDD));

    assertThat(rddPaths).hasSize(0);
  }

  @Test
  void testFindRDDPathsUnknownRdd() {
    assertThat(PlanUtils.findRDDPaths(Collections.singletonList(mock(RDD.class)))).isEmpty();
  }
}
