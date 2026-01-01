/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

import lombok.Getter;
import lombok.Setter;

/**
 * Status of the OpenLineage run metadata, indicating whether inputs and outputs were detected. This
 * is used to determine the quality of the lineage data collected during a Spark job run.
 */
@Getter
@Setter
public class OpenLineageRunStatus {
  private boolean detectedInputs;
  private boolean detectedOutputs;

  public void capturedInputs(int datasetsCount) {
    if (datasetsCount > 0) {
      detectedInputs = true;
    }
  }

  public void capturedOutputs(int datasetsCount) {
    if (datasetsCount > 0) {
      detectedOutputs = true;
    }
  }
}
