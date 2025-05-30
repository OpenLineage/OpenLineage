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
  String smartDebugEnabled;

  public static final String ENABLED = "enabled";

  @JsonProperty("smartMode")
  String mode;

  public boolean isSmartDebugActive(boolean emptyInputs, boolean emptyOutputs) {
    if (!isSmartDebugEnabled()) {
      return false;
    }

    if (ANY_MISSING.equalsIgnoreCase(mode)) {
      return (emptyInputs || emptyOutputs);
    } else if (OUTPUT_MISSING.equalsIgnoreCase(mode)) {
      return emptyOutputs;
    } else {
      log.warn("Unknown mode: {}", mode);
      return false;
    }
  }

  public boolean isSmartDebugEnabled() {
    return ENABLED.equalsIgnoreCase(smartDebugEnabled);
  }

  // TODO: add ability to switch off metrics
  // TODO: add limit on the debug facet payload - extra setting
}
