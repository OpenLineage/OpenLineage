/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan.column;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.Getter;

/**
 * Column level lineage of a checkpointed plan, captured while the plan was still available (see
 * {@link io.openlineage.spark.api.Checkpoint}). It's resolved all the way down to the fields of the
 * datasets read by the plan - including ones read through other checkpoints - so it stays valid on
 * its own, regardless of whether lineage of any checkpoint it was built from is still kept around.
 *
 * <p>Field-level and dataset-level dependencies are kept separate regardless of configuration. The
 * plan reading the checkpoint decides how to combine them, the same way it does for its own ones.
 */
@Getter
public class CheckpointColumnLineage {

  private static final CheckpointColumnLineage NO_LINEAGE =
      new CheckpointColumnLineage(Collections.emptyMap(), Collections.emptyList());

  /** Inputs used to compute each output field of the checkpointed plan, keyed by field name. */
  private final Map<String, List<TransformedInput>> fieldInputs;

  /**
   * Inputs the checkpointed plan depends on as a whole, i.e. columns used for filtering, sorting,
   * grouping, joining, window functions, etc., which are not tied to any specific output field (and
   * may not even appear in the checkpoint's output schema at all).
   */
  private final List<TransformedInput> datasetDependencyInputs;

  CheckpointColumnLineage(
      Map<String, List<TransformedInput>> fieldInputs,
      List<TransformedInput> datasetDependencyInputs) {
    this.fieldInputs = Collections.unmodifiableMap(fieldInputs);
    this.datasetDependencyInputs = Collections.unmodifiableList(datasetDependencyInputs);
  }

  public static CheckpointColumnLineage empty() {
    return NO_LINEAGE;
  }

  public boolean hasField(String fieldName) {
    return fieldInputs.containsKey(fieldName);
  }

  List<TransformedInput> getFieldInputs(String fieldName) {
    return fieldInputs.getOrDefault(fieldName, Collections.emptyList());
  }
}
