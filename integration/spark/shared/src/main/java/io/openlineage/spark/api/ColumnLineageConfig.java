/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@AllArgsConstructor
@Setter
@Getter
public class ColumnLineageConfig {
  /**
   * Determines if the dataset dependencies (FILTER, SORT BY, GROUP BY, WINDOW, etc.) should be
   * represented as field dependencies. WARNING: This flag is temporary. It is going to default to
   * true in future versions and eventually removed.
   */
  // TODO #3084: For the release 1.26.0 this flag should default to true
  // TODO #3084: Three releases later (1.29.0), this flag should be removed and the behavior should
  // reflect it set to true
  private boolean datasetLineageEnabled;
}
