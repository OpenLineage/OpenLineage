/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.column;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openlineage.client.OpenLineage;

public class ColumnLevelLineageTestUtils {

  static void assertColumnDependsOn(
      OpenLineage.ColumnLineageDatasetFacet facet,
      String outputColumn,
      String expectedNamespace,
      String expectedName,
      String expectedInputField) {
    assertTrue(
        facet.getFields().getAdditionalProperties().get(outputColumn).getInputFields().stream()
            .filter(f -> f.getNamespace().equalsIgnoreCase(expectedNamespace))
            .filter(f -> f.getName().endsWith(expectedName))
            .filter(f -> f.getField().equalsIgnoreCase(expectedInputField))
            .findAny()
            .isPresent());
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
