/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan.column;

import io.openlineage.client.utils.TransformationInfo;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;

/**
 * Reference to an output field of a checkpointed plan read by the plan being processed. Only
 * resolved into the inputs captured for it (see {@link CheckpointColumnLineage}) once a dependency
 * of an output field of the current plan actually reaches it.
 */
@AllArgsConstructor
@EqualsAndHashCode
class CheckpointField {
  private final CheckpointColumnLineage lineage;
  private final String fieldName;

  /**
   * Combines the transformations captured for the field's inputs with the transformation of a
   * dependency found within the current plan, the same way {@link Dependency#merge} combines
   * dependencies.
   */
  Stream<TransformedInput> resolve(Dependency dependency, boolean descriptionsEnabled) {
    TransformationInfo downstream = dependency.getTransformationInfo();
    return lineage.getFieldInputs(fieldName).stream()
        .map(
            captured -> {
              if (downstream == null || captured.getTransformationInfo() == null) {
                return new TransformedInput(captured.getInput(), downstream);
              }
              TransformationInfo merged =
                  downstream.merge(
                      captured.getTransformationInfo(),
                      (outer, inner) -> descriptionsEnabled ? outer.replace(fieldName, inner) : "");
              return new TransformedInput(captured.getInput(), merged);
            });
  }
}
