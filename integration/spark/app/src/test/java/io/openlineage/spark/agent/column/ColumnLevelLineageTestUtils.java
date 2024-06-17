/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.column;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.plan.column.TransformationInfo;
import java.util.List;
import java.util.stream.Collectors;

public class ColumnLevelLineageTestUtils {

  static void assertColumnDependsOn(
      OpenLineage.ColumnLineageDatasetFacet facet,
      String outputColumn,
      String expectedNamespace,
      String expectedName,
      String expectedInputField) {
    assertTrue(
        facet.getFields().getAdditionalProperties().get(outputColumn).getInputFields().stream()
            .anyMatch(
                f ->
                    f.getNamespace().equalsIgnoreCase(expectedNamespace)
                        && f.getName().endsWith(expectedName)
                        && f.getField().equalsIgnoreCase(expectedInputField)));
  }

  static void assertColumnDependsOnType(
      OpenLineage.ColumnLineageDatasetFacet facet,
      String outputColumn,
      String expectedNamespace,
      String expectedName,
      String expectedInputField,
      TransformationInfo transformation) {
    assertTrue(
        hasInputField(
            facet,
            expectedNamespace,
            expectedName,
            expectedInputField,
            transformation,
            outputColumn));
  }

  static void assertAllColumnsDependsOnType(
      OpenLineage.ColumnLineageDatasetFacet facet,
      List<String> outputColumns,
      String expectedNamespace,
      String expectedName,
      String expectedInputField,
      TransformationInfo transformation) {
    assertTrue(
        outputColumns.stream()
            .allMatch(
                outputColumn ->
                    hasInputField(
                        facet,
                        expectedNamespace,
                        expectedName,
                        expectedInputField,
                        transformation,
                        outputColumn)));
  }

  private static boolean hasInputField(
      OpenLineage.ColumnLineageDatasetFacet facet,
      String expectedNamespace,
      String expectedName,
      String expectedInputField,
      TransformationInfo transformation,
      String outputColumn) {
    return facet.getFields().getAdditionalProperties().get(outputColumn).getInputFields().stream()
        .anyMatch(
            f ->
                f.getNamespace().equalsIgnoreCase(expectedNamespace)
                    && f.getName().endsWith(expectedName)
                    && f.getField().equalsIgnoreCase(expectedInputField)
                    && f.getTransformations().stream()
                        .map(
                            t ->
                                new TransformationInfo(
                                    TransformationInfo.Types.valueOf(t.getType()),
                                    TransformationInfo.Subtypes.valueOf(t.getSubtype()),
                                    t.getDescription(),
                                    t.getMasking()))
                        .collect(Collectors.toSet())
                        .contains(transformation));
  }

  static void assertColumnDependsOnInputs(
      OpenLineage.ColumnLineageDatasetFacet facet,
      String outputColumn,
      int expectedAmountOfInputs) {

    assertEquals(
        expectedAmountOfInputs,
        facet.getFields().getAdditionalProperties().get(outputColumn).getInputFields().size());
  }
}
