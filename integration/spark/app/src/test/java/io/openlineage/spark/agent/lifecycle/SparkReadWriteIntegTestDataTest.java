/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SparkReadWriteIntegTestDataTest {

  @Test
  void testOverlappingAttemptsKeepTheirFilesIsolated(@TempDir Path bucket) throws IOException {
    List<String> firstAttempt =
        SparkReadWriteIntegTest.createExternalRDDDatasetNames("3.2.4", "2.12");
    List<String> secondAttempt =
        SparkReadWriteIntegTest.createExternalRDDDatasetNames("3.2.4", "2.12");

    try (FileSystem fileSystem = FileSystem.newInstanceLocal(new Configuration())) {
      for (String datasetName : firstAttempt) {
        Files.createDirectories(bucket.resolve(datasetName));
        Files.write(bucket.resolve(datasetName).resolve("part-00000"), new byte[] {1});
      }
      for (String datasetName : secondAttempt) {
        Files.createDirectories(bucket.resolve(datasetName));
        Files.write(bucket.resolve(datasetName).resolve("part-00000"), new byte[] {2});
      }

      SparkReadWriteIntegTest.cleanupExternalRDDTestData(
          fileSystem, bucket.toUri().toString(), firstAttempt);

      for (String datasetName : firstAttempt) {
        assertThat(bucket.resolve(datasetName)).doesNotExist();
      }
      for (String datasetName : secondAttempt) {
        assertThat(Files.readAllBytes(bucket.resolve(datasetName).resolve("part-00000")))
            .containsExactly((byte) 2);
      }

      SparkReadWriteIntegTest.cleanupExternalRDDTestData(
          fileSystem, bucket.toUri().toString(), secondAttempt);
      try (java.util.stream.Stream<Path> remainingPaths = Files.list(bucket)) {
        assertThat(remainingPaths).isEmpty();
      }
    }
  }

  @Test
  void testCleanupFailurePreservesTestFailureAndAttemptsAllPaths() throws IOException {
    FileSystem fileSystem = mock(FileSystem.class);
    String bucketUrl = "s3a://test-bucket";
    List<String> datasetNames =
        SparkReadWriteIntegTest.createExternalRDDDatasetNames("3.2.4", "2.12");
    doThrow(new IOException("cleanup failed"))
        .when(fileSystem)
        .delete(new org.apache.hadoop.fs.Path(bucketUrl + "/" + datasetNames.get(0)), true);
    AssertionError testFailure = new AssertionError("lineage assertion failed");

    assertThatThrownBy(
            () -> {
              try {
                throw testFailure;
              } finally {
                SparkReadWriteIntegTest.cleanupExternalRDDTestData(
                    fileSystem, bucketUrl, datasetNames);
              }
            })
        .isSameAs(testFailure);

    for (String datasetName : datasetNames) {
      verify(fileSystem).delete(new org.apache.hadoop.fs.Path(bucketUrl + "/" + datasetName), true);
    }
  }
}
