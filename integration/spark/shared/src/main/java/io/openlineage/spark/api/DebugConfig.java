/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@NoArgsConstructor
@AllArgsConstructor
@Setter
@Getter
@Slf4j
public class DebugConfig {

  public static String ANY_MISSING = "any-missing";
  public static String OUTPUT_MISSING = "output-missing";

  @JsonProperty("smart")
  boolean smartEnabled;

  @JsonProperty("smartMode")
  String mode;

  @JsonProperty("metricsDisabled")
  boolean metricsDisabled;

  @JsonProperty Integer payloadSizeLimitInKilobytes;

  public boolean isSmartDebugActive(boolean hasDetectedInputs, boolean hasDetectedOutputs) {
    if (!smartEnabled) {
      return false;
    }

    if (ANY_MISSING.equalsIgnoreCase(mode)) {
      return isAnyMissingActive(hasDetectedInputs, hasDetectedOutputs);
    } else if (OUTPUT_MISSING.equalsIgnoreCase(mode)) {
      return !hasDetectedOutputs;
    } else {
      if (mode != null && !mode.isEmpty()) {
        log.warn("Unsupported smart mode: {}. Falling back to any-missing mode.", mode);
      }
      return isAnyMissingActive(hasDetectedInputs, hasDetectedOutputs);
    }
  }

  private boolean isAnyMissingActive(boolean hasDetectedInputs, boolean hasDetectedOutputs) {
    return (!hasDetectedInputs || !hasDetectedOutputs);
  }
}
