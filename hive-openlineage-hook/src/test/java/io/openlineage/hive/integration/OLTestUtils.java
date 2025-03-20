/*
 * Copyright 2024 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openlineage.hive.integration;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
import io.openlineage.hive.hooks.TransformationInfo;
import io.openlineage.hive.transport.DummyTransport;
import java.util.List;
import java.util.stream.Collectors;

public class OLTestUtils {

  static void assertNumEvents(int numEvents) {
    List<OpenLineage.BaseEvent> olEvents = DummyTransport.getEvents();
    assertThat(olEvents).hasSize(numEvents);
  }

  static void assertLastEventIsOfType(EventType eventType) {
    RunEvent event = DummyTransport.getLastEvent();
    assertThat(event.getEventType()).isEqualTo(eventType);
  }

  static void assertCreatedSingleEventOfType(EventType eventType) {
    assertNumEvents(1);
    assertLastEventIsOfType(eventType);
  }

  static void assertColumnDependsOn(
      OpenLineage.ColumnLineageDatasetFacet facet,
      String outputColumn,
      String expectedNamespace,
      String expectedName,
      String expectedInputField) {
    assertThat(
            facet.getFields().getAdditionalProperties().get(outputColumn).getInputFields().stream()
                .anyMatch(
                    f ->
                        f.getNamespace().equalsIgnoreCase(expectedNamespace)
                            && f.getName().endsWith(expectedName)
                            && f.getField().equalsIgnoreCase(expectedInputField)))
        .isTrue();
  }

  static void assertColumnDependsOnType(
      OpenLineage.ColumnLineageDatasetFacet facet,
      String outputColumn,
      String expectedNamespace,
      String expectedName,
      String expectedInputField,
      TransformationInfo transformation) {
    assertThat(
            hasInputField(
                facet,
                expectedNamespace,
                expectedName,
                expectedInputField,
                transformation,
                outputColumn))
        .isTrue();
  }

  static void assertDatasetDependsOnType(
      OpenLineage.ColumnLineageDatasetFacet facet,
      String expectedNamespace,
      String expectedName,
      String expectedInputField,
      TransformationInfo transformation) {
    assertThat(
            hasDatasetInputField(
                facet, expectedNamespace, expectedName, expectedInputField, transformation))
        .isTrue();
  }

  static void assertAllColumnsDependsOnType(
      OpenLineage.ColumnLineageDatasetFacet facet,
      List<String> outputColumns,
      String expectedNamespace,
      String expectedName,
      String expectedInputField,
      TransformationInfo transformation) {
    assertThat(
            outputColumns.stream()
                .allMatch(
                    outputColumn ->
                        hasInputField(
                            facet,
                            expectedNamespace,
                            expectedName,
                            expectedInputField,
                            transformation,
                            outputColumn)))
        .isTrue();
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
    assertThat(expected).isEqualTo(countColumnDependencies(facet));
  }

  static void assertCountColumnDependencies(
      OpenLineage.ColumnLineageDatasetFacet facet, String outputColumn, int expected) {
    assertThat(expected).isEqualTo(countColumnDependencies(facet, outputColumn));
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
    assertThat(expected).isEqualTo(countDatasetDependencies(facet));
  }
}
