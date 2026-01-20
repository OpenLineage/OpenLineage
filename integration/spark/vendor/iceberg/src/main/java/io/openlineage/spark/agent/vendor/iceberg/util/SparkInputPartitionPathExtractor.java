/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.iceberg.util;

import static io.openlineage.spark.agent.util.ReflectionUtils.tryExecuteMethod;

import io.openlineage.spark.agent.util.InputPartitionPathExtractor;
import io.openlineage.spark.agent.vendor.iceberg.IcebergVendor;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.spark.sql.connector.read.InputPartition;

@Slf4j
public class SparkInputPartitionPathExtractor implements InputPartitionPathExtractor {
  private static final String SPARK_INPUT_PARTITION =
      "org.apache.iceberg.spark.source.SparkInputPartition";

  @Override
  public boolean isDefinedAt(InputPartition inputPartition) {
    if (!new IcebergVendor().isVendorAvailable()) {
      return false;
    }

    try {
      Class<?> sparkInputPartitionClass = Class.forName(SPARK_INPUT_PARTITION);
      return sparkInputPartitionClass.isInstance(inputPartition);
    } catch (Exception e) {
      if (log.isDebugEnabled()) {
        log.debug("Could not load {}: {}", SPARK_INPUT_PARTITION, e.getMessage());
      }
      return false;
    }
  }

  @Override
  public List<Path> extract(Configuration conf, InputPartition inputPartition) {
    Optional<Object> taskGroup = tryExecuteMethod(inputPartition, "taskGroup");
    if (!taskGroup.isPresent() || !(taskGroup.get() instanceof ScanTaskGroup)) {
      return Collections.emptyList();
    }

    FileSystem fileSystem;
    try {
      fileSystem = FileSystem.get(conf);
    } catch (Exception e) {
      if (log.isDebugEnabled()) {
        log.debug("Failed to obtain FileSystem: {}", e.getMessage());
      }
      return Collections.emptyList();
    }

    ScanTaskGroup<?> scanTaskGroup = (ScanTaskGroup<?>) taskGroup.get();
    return scanTaskGroup.tasks().stream()
        .filter(t -> t instanceof ContentScanTask)
        .map(t -> ((ContentScanTask<?>) t).file().path().toString())
        .map(p -> qualifyPath(fileSystem, p))
        .filter(Objects::nonNull)
        .map(this::getParent)
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  private Path qualifyPath(FileSystem fileSystem, String pathString) {
    // Qualify the path to ensure it has the correct scheme,
    // path without a scheme (e.g. "/tmp/source/file.parquet") might become
    // "hdfs:///tmp/source/file.parquet", "file:///tmp/source/file.parquet" or something else,
    // depending on the FileSystem configuration
    try {
      Path path = new Path(pathString);
      if (path.toUri().getScheme() != null) {
        return path;
      }
      return fileSystem.makeQualified(path);
    } catch (Exception e) {
      if (log.isDebugEnabled()) {
        log.debug("Failed to qualify path {}: {}", pathString, e.getMessage());
      }
      return null;
    }
  }

  private Path getParent(Path path) {
    try {
      Path parent = path.getParent();
      if (parent == null) {
        return null;
      }

      if ("data".equals(parent.getName())) {
        return parent.getParent();
      }

      return parent;
    } catch (Exception e) {
      if (log.isDebugEnabled()) {
        log.debug("Failed to normalize path {}: {}", path, e.getMessage());
      }
      return null;
    }
  }
}
