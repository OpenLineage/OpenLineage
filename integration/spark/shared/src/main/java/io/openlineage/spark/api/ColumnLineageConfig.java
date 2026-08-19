/*
/* Copyright 2018-2026 contributors to the OpenLineage project
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
  // TODO #3084: Three releases later (1.29.0), this flag should be removed and the behavior should
  // reflect it set to true
  private Boolean datasetLineageEnabled;

  private Integer schemaSizeLimit;

  /**
   * Determines whether a pessimistic fan-in is emitted across a typed Dataset boundary (the
   * {@code DeserializeToObject} / {@code SerializeFromObject} pair produced by {@code map},
   * {@code mapPartitions}, {@code flatMap} and {@code mapGroups}).
   *
   * <p>A typed operation hands the whole row to a lambda as a single JVM object, so per-field
   * lineage is not recoverable from the logical plan. When this flag is enabled every output field
   * of the boundary is reported as INDIRECT/TRANSFORMATION dependent on every input field the
   * deserializer read - over-broad, but never asserting a specific false pairing. When it is
   * disabled the boundary stays silent, which is the historical behaviour.
   *
   * <p>Defaults to {@code false}: the fan-in ships dark until there is field evidence that its
   * breadth is acceptable to consumers.
   */
  private Boolean typedBoundaryFanInEnabled;

  /**
   * Upper bound on the number of edges a single typed boundary may contribute, i.e. on
   * {@code outputFieldCount * inputFieldCount}. Above the bound the boundary emits <b>nothing</b>
   * rather than a fan-in.
   *
   * <p>This is a deliberate no-emit, not a truncation. {@code ColumnLevelLineageBuilder} discards
   * the whole column lineage facet once the returned input fields exceed its own
   * {@code RETURNED_INPUT_FIELD_LIMIT} of 100 000, so an uncapped fan-in over a wide table turns
   * "too much lineage" into "no lineage" for the entire dataset at around 320 columns. Emitting
   * nothing for the one operator that cannot be traced anyway keeps the rest of the facet intact.
   *
   * <p>Defaults to 10 000, which is a 100x100 boundary.
   */
  private Integer typedBoundaryFanInMaxEdges;
}
