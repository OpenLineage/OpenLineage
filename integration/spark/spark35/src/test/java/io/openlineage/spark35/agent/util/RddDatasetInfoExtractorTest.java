/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark35.agent.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.util.Collections;
import java.util.List;
import org.apache.spark.paths.SparkPath;
import org.apache.spark.sql.execution.datasources.FilePartition;
import org.apache.spark.sql.execution.datasources.FileScanRDD;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class RddDatasetInfoExtractorTest {

  @Test
  void extractsPathsFromSparkPath() {
    assertThat(extract(file("/source/part.parquet")))
        .extracting(DatasetIdentifier::getNamespace, DatasetIdentifier::getName)
        .containsExactly(tuple("file", "/source"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"nullFilePath", "filePathFailure", "nullPath", "pathFailure", "rootPath"})
  void skipsUnavailablePathAndRetainsNeighboringFiles(String failure) {
    PartitionedFile unavailable = mock(PartitionedFile.class);
    SparkPath sparkPath = mock(SparkPath.class);
    switch (failure) {
      case "nullFilePath":
        when(unavailable.filePath()).thenReturn(null);
        break;
      case "filePathFailure":
        when(unavailable.filePath()).thenThrow(new IllegalStateException("filePath unavailable"));
        break;
      case "nullPath":
        when(unavailable.filePath()).thenReturn(sparkPath);
        when(sparkPath.toPath()).thenReturn(null);
        break;
      case "pathFailure":
        when(unavailable.filePath()).thenReturn(sparkPath);
        when(sparkPath.toPath()).thenThrow(new IllegalStateException("toPath unavailable"));
        break;
      case "rootPath":
        unavailable = file("/");
        break;
      default:
        throw new IllegalArgumentException(failure);
    }

    assertThat(extract(file("/before/part.parquet"), unavailable, file("/after/part.parquet")))
        .extracting(DatasetIdentifier::getNamespace, DatasetIdentifier::getName)
        .containsExactly(tuple("file", "/before"), tuple("file", "/after"));
  }

  @Test
  void returnsEmptyWhenNoFilePathsAreAvailable() {
    assertThat(extract(mock(PartitionedFile.class))).isEmpty();
  }

  private static PartitionedFile file(String path) {
    PartitionedFile file = mock(PartitionedFile.class);
    when(file.filePath()).thenReturn(SparkPath.fromPathString(path));
    return file;
  }

  private static List<DatasetIdentifier> extract(PartitionedFile... files) {
    FileScanRDD rdd = mock(FileScanRDD.class);
    FilePartition partition = mock(FilePartition.class);
    when(partition.files()).thenReturn(files);
    when(rdd.filePartitions())
        .thenReturn(ScalaConversionUtils.fromList(Collections.singletonList(partition)));
    return PlanUtils.findDatasetIdentifiers(Collections.singletonList(rdd));
  }
}
