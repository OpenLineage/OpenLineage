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

  static void assertDatasetDependsOnType(
      OpenLineage.ColumnLineageDatasetFacet facet,
      String expectedNamespace,
      String expectedName,
      String expectedInputField,
      TransformationInfo transformation) {
    assertTrue(
        hasDatasetInputField(
            facet, expectedNamespace, expectedName, expectedInputField, transformation));
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
            f -> isField(expectedNamespace, expectedName, expectedInputField, transformation, f));
  }

  private static boolean hasDatasetInputField(
      OpenLineage.ColumnLineageDatasetFacet facet,
      String expectedNamespace,
      String expectedName,
      String expectedInputField,
      TransformationInfo transformation) {
    return facet.getDataset().stream()
        .anyMatch(
            f -> isField(expectedNamespace, expectedName, expectedInputField, transformation, f));
  }

  private static boolean isField(
      String expectedNamespace,
      String expectedName,
      String expectedInputField,
      TransformationInfo transformation,
      OpenLineage.InputField f) {
    return f.getNamespace().equalsIgnoreCase(expectedNamespace)
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
            .contains(transformation);
  }

  static void assertColumnDependsOnInputs(
      OpenLineage.ColumnLineageDatasetFacet facet,
      String outputColumn,
      int expectedAmountOfInputs) {

    assertEquals(
        expectedAmountOfInputs,
        facet.getFields().getAdditionalProperties().get(outputColumn).getInputFields().size());
  }

  static int countColumnDependencies(OpenLineage.ColumnLineageDatasetFacet facet) {
    return countColumnDependencies(facet, null);
  }

  static int countColumnDependencies(
      OpenLineage.ColumnLineageDatasetFacet facet, String outputColumn) {
    int count = 0;
    for (String column : facet.getFields().getAdditionalProperties().keySet()) {
      if (outputColumn == null || column.equals(outputColumn)) {
        List<OpenLineage.InputField> inputFields =
            facet.getFields().getAdditionalProperties().get(column).getInputFields();
        for (OpenLineage.InputField inputField : inputFields) {
          count += inputField.getTransformations().size();
        }
      }
    }
    return count;
  }

  static void assertCountColumnDependencies(
      OpenLineage.ColumnLineageDatasetFacet facet, int expected) {
    assertEquals(expected, countColumnDependencies(facet));
  }

  static void assertCountColumnDependencies(
      OpenLineage.ColumnLineageDatasetFacet facet, String outputColumn, int expected) {
    assertEquals(expected, countColumnDependencies(facet, outputColumn));
  }

  static int countDatasetDependencies(OpenLineage.ColumnLineageDatasetFacet facet) {
    int count = 0;
    List<OpenLineage.InputField> inputFields = facet.getDataset();
    if (inputFields != null) {
      for (OpenLineage.InputField inputField : inputFields) {
        count += inputField.getTransformations().size();
      }
    }
    return count;
  }

  static void assertCountDatasetDependencies(
      OpenLineage.ColumnLineageDatasetFacet facet, int expected) {
    assertEquals(expected, countDatasetDependencies(facet));
  }
}
