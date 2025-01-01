/*
/* Copyright 2018-2025 contributors to the OpenLineage project
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
public class IcebergScanMetrics {
  Long totalPlanningDuration;
  Long resultDataFiles;
  Long resultDeleteFiles;
  Long totalDataManifests;
  Long totalDeleteManifests;
  Long scannedDataManifests;
  Long skippedDataManifests;
  Long totalFileSizeInBytes;
  Long totalDeleteFileSizeInBytes;
  Long skippedDataFiles;
  Long skippedDeleteFiles;
  Long scannedDeleteManifests;
  Long skippedDeleteManifests;
  Long indexedDeleteFiles;
  Long equalityDeleteFiles;
  Long positionalDeleteFiles;
}
