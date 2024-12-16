/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Builder
@EqualsAndHashCode
public class IcebergCommitMetrics {
  Long totalDuration;
  Long attempts;
  Long addedDataFiles;
  Long removedDataFiles;
  Long totalDataFiles;
  Long addedDeleteFiles;
  Long addedEqualityDeleteFiles;
  Long addedPositionalDeleteFiles;
  Long addedDVs;
  Long removedDeleteFiles;
  Long removedEqualityDeleteFiles;
  Long removedPositionalDeleteFiles;
  Long removedDVs;
  Long totalDeleteFiles;
  Long addedRecords;
  Long removedRecords;
  Long totalRecords;
  Long addedFilesSizeInBytes;
  Long removedFilesSizeInBytes;
  Long totalFilesSizeInBytes;
  Long addedPositionalDeletes;
  Long removedPositionalDeletes;
  Long totalPositionalDeletes;
  Long addedEqualityDeletes;
  Long removedEqualityDeletes;
  Long totalEqualityDeletes;
}
