/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.MapPartitionsRDD;
import org.apache.spark.rdd.NewHadoopRDD;
import org.apache.spark.rdd.ParallelCollectionRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.rdd.UnionRDD;
import org.apache.spark.sql.execution.datasources.FilePartition;
import org.apache.spark.sql.execution.datasources.FileScanRDD;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.mutable.ArrayBuffer;

class RddPathUtilsTest {

  private static final String DATA_FIELD_NAME = "data";

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

    FieldUtils.writeDeclaredField(parallelCollectionRDD, DATA_FIELD_NAME, data, true);

    List rddPaths = PlanUtils.findRDDPaths(Collections.singletonList(parallelCollectionRDD));

    assertThat(rddPaths).hasSize(2);
    assertThat(rddPaths.get(0).toString()).isEqualTo("/some-path1");
    assertThat(rddPaths.get(1).toString()).isEqualTo("/some-path2");
  }

  @Test
  void testFindRDDPathsForUnionRDD() throws IllegalAccessException {
    UnionRDD unionRdd = mock(UnionRDD.class);
    ParallelCollectionRDD rdd1 = mock(ParallelCollectionRDD.class);
    ParallelCollectionRDD rdd2 = mock(ParallelCollectionRDD.class);

    Seq<Tuple2<String, Integer>> data1 =
        JavaConverters.asScalaIteratorConverter(
                Arrays.asList(new Tuple2<>("/some-path2/data-file-654342.snappy.parquet", 345))
                    .iterator())
            .asScala()
            .toSeq();
    Seq<Tuple2<String, Integer>> data2 =
        JavaConverters.asScalaIteratorConverter(
                Arrays.asList(new Tuple2<>("/some-path1/data-file-325342.snappy.parquet", 345))
                    .iterator())
            .asScala()
            .toSeq();
    FieldUtils.writeDeclaredField(rdd1, "data", data1, true);
    FieldUtils.writeDeclaredField(rdd2, "data", data2, true);

    when(unionRdd.rdds()).thenReturn(ScalaConversionUtils.fromList(Arrays.asList(rdd1, rdd2)));

    List rddPaths = PlanUtils.findRDDPaths(Collections.singletonList(unionRdd));
    assertThat(rddPaths)
        .hasSize(2)
        .map(o -> ((Path) o).toString())
        .containsExactlyInAnyOrder("/some-path1", "/some-path2");
  }

  @Test
  void testNewHadoopRDDExtractor() throws IllegalAccessException {
    SparkConf sparkConf = new SparkConf();
    sparkConf.setMaster("local[*]").setAppName("test");
    SparkContext sparkContext = new SparkContext(sparkConf);
    NewHadoopRDD rdd = new NewHadoopRDD(sparkContext, null, null, null, new Configuration());

    try (MockedStatic<FileInputFormat> ignored = mockStatic(FileInputFormat.class)) {
      when(FileInputFormat.getInputPaths(any()))
          .thenReturn(new Path[] {new Path("some-path1"), new Path("some-path2")});

      List rddPaths = PlanUtils.findRDDPaths(Collections.singletonList(rdd));
      assertThat(rddPaths)
          .hasSize(2)
          .map(o -> ((Path) o).toString())
          .containsExactlyInAnyOrder("some-path1", "some-path2");
    }
    sparkContext.stop();
  }

  @Test
  void testFindRDDPathsForParallelCollectionRDDWhenNoDataField() throws IllegalAccessException {
    ParallelCollectionRDD parallelCollectionRDD = mock(ParallelCollectionRDD.class);

    FieldUtils.writeDeclaredField(parallelCollectionRDD, DATA_FIELD_NAME, null, true);
    assertThat(PlanUtils.findRDDPaths(Collections.singletonList(parallelCollectionRDD))).hasSize(0);
  }

  @Test
  void testFindRDDPathsForParallelCollectionRDDWhenDataFieldNotSeqOfTuples()
      throws IllegalAccessException {
    ParallelCollectionRDD parallelCollectionRDD = mock(ParallelCollectionRDD.class);
    Seq<Integer> data =
        JavaConverters.asScalaIteratorConverter(Arrays.asList(333).iterator()).asScala().toSeq();

    FieldUtils.writeDeclaredField(parallelCollectionRDD, DATA_FIELD_NAME, data, true);
    assertThat(PlanUtils.findRDDPaths(Collections.singletonList(parallelCollectionRDD))).hasSize(0);
  }

  @Test
  void testFindRDDPathsForParallelCollectionRDDWhenDataFieldIsArrayBuffer()
      throws IllegalAccessException {
    Assumptions.assumeTrue(
        FieldUtils.getDeclaredField(ParallelCollectionRDD.class, DATA_FIELD_NAME, true)
            .getType()
            .isAssignableFrom(ArrayBuffer.class),
        "Skipping test: ArrayBuffer not assignable to Seq (Scala 2.13+)");

    ParallelCollectionRDD<?> parallelCollectionRDD = mock(ParallelCollectionRDD.class);
    ArrayBuffer<Path> data = new ArrayBuffer<>();
    Path path1 = new Path("/some-path1/part-9-256964}");
    Path path2 = new Path("/some-path2/part-5-123964");
    Path path3 = new Path("/some-path2/part-2-453964");
    data.appendAll(
        JavaConverters.asScalaBufferConverter(Arrays.asList(path1, path2, path3)).asScala());

    FieldUtils.writeDeclaredField(parallelCollectionRDD, DATA_FIELD_NAME, data, true);
    List<?> rddPaths = PlanUtils.findRDDPaths(Collections.singletonList(parallelCollectionRDD));

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
        .thenReturn(ScalaConversionUtils.fromList(Collections.singletonList(filePartition)));

    List rddPaths = PlanUtils.findRDDPaths(Collections.singletonList(fileScanRDD));

    assertThat(rddPaths).hasSize(0);
  }

  @Test
  void testFindRDDPathsUnknownRdd() {
    assertThat(PlanUtils.findRDDPaths(Collections.singletonList(mock(RDD.class)))).isEmpty();
  }
}
