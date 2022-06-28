/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.ColumnLineageDatasetFacetFields;
import io.openlineage.client.Utils;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.catalyst.expressions.ExprId;

/**
 * Builder class used to store information required to build {@link
 * ColumnLineageDatasetFacetFields}. Single instance of the class is passed when traversing logical
 * plan. It stores input fields, output fields and dependencies between the expressions in {@link
 * org.apache.spark.sql.catalyst.plans.logical.LogicalPlan}. Dependency between expressions are used
 * to identify inputs used to evaluate specific output field.
 */
@Slf4j
public class ColumnLevelLineageBuilder {

  private Map<ExprId, Set<ExprId>> exprDependencies = new HashMap<>();
  private Map<ExprId, List<Pair<DatasetIdentifier, String>>> inputs = new HashMap<>();
  private Map<OpenLineage.SchemaDatasetFacetFields, ExprId> outputs = new HashMap<>();
  private final OpenLineage.SchemaDatasetFacet schema;
  private final OpenLineageContext context;

  ColumnLevelLineageBuilder(OpenLineage.SchemaDatasetFacet schema, OpenLineageContext context) {
    this.schema = schema;
    this.context = context;
  }

  /**
   * Adds input field.
   *
   * @param exprId
   * @param datasetIdentifier
   * @param attributeName
   */
  public void addInput(ExprId exprId, DatasetIdentifier datasetIdentifier, String attributeName) {
    inputs
        .computeIfAbsent(exprId, k -> new LinkedList<>())
        .add(Pair.of(datasetIdentifier, attributeName));
  }

  /**
   * Adds output field.
   *
   * @param exprId
   * @param attributeName
   */
  public void addOutput(ExprId exprId, String attributeName) {
    schema.getFields().stream()
        .filter(field -> field.getName().equals(attributeName))
        .findAny()
        .ifPresent(field -> outputs.put(field, exprId));
  }

  /**
   * Add dependency between parent expression and child expression. Evaluation of parent requires
   * child.
   *
   * @param parent
   * @param child
   */
  public void addDependency(ExprId parent, ExprId child) {
    exprDependencies.computeIfAbsent(parent, k -> new HashSet<>()).add(child);
  }

  public boolean hasOutputs() {
    return !outputs.isEmpty();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    ObjectMapper mapper = Utils.newObjectMapper();
    try {
      sb.append("Inputs: ")
          .append(mapper.writeValueAsString(inputs))
          .append(System.lineSeparator());
      sb.append("Dependencies: ")
          .append(
              mapper.writeValueAsString(
                  exprDependencies.entrySet().stream()
                      .collect(
                          Collectors.toMap(
                              Map.Entry::getKey,
                              e -> e.toString())) // need to call toString method explicitly
                  ))
          .append(System.lineSeparator());

      sb.append("Outputs: ")
          .append(
              mapper.writeValueAsString(
                  outputs.entrySet().stream()
                      .collect(
                          Collectors.toMap(
                              Map.Entry::getKey,
                              e -> e.toString())) // need to call toString method explicitly
                  ))
          .append(System.lineSeparator());
    } catch (JsonProcessingException e) {
      sb.append("Unable to serialize: ").append(e.toString());
    }

    return sb.toString();
  }

  /**
   * Builds {@link ColumnLineageDatasetFacetFields} to be included in dataset facet.
   *
   * @return
   */
  public ColumnLineageDatasetFacetFields build() {
    OpenLineage.ColumnLineageDatasetFacetFieldsBuilder fieldsBuilder =
        context.getOpenLineage().newColumnLineageDatasetFacetFieldsBuilder();

    schema.getFields().stream()
        .map(field -> Pair.of(field, getInputsUsedFor(field.getName())))
        .filter(pair -> !pair.getRight().isEmpty())
        .map(pair -> Pair.of(pair.getLeft(), facetInputFields(pair.getRight())))
        .forEach(
            pair ->
                fieldsBuilder.put(
                    pair.getLeft().getName(),
                    context
                        .getOpenLineage()
                        .newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                        .inputFields(pair.getRight())
                        .build()));

    return fieldsBuilder.build();
  }

  private List<OpenLineage.ColumnLineageDatasetFacetFieldsAdditionalInputFields> facetInputFields(
      List<Pair<DatasetIdentifier, String>> inputFields) {
    return inputFields.stream()
        .map(
            field ->
                new OpenLineage.ColumnLineageDatasetFacetFieldsAdditionalInputFieldsBuilder()
                    .namespace(field.getLeft().getNamespace())
                    .name(field.getLeft().getName())
                    .field(field.getRight())
                    .build())
        .collect(Collectors.toList());
  }

  List<Pair<DatasetIdentifier, String>> getInputsUsedFor(String outputName) {
    Optional<OpenLineage.SchemaDatasetFacetFields> outputField =
        schema.getFields().stream()
            .filter(field -> field.getName().equalsIgnoreCase(outputName))
            .findAny();

    if (!outputField.isPresent() || !outputs.containsKey(outputField.get())) {
      return Collections.emptyList();
    }

    return findDependentInputs(outputs.get(outputField.get())).stream()
        .filter(inputExprId -> inputs.containsKey(inputExprId))
        .flatMap(inputExprId -> inputs.get(inputExprId).stream())
        .filter(Objects::nonNull)
        .distinct()
        .collect(Collectors.toList());
  }

  private List<ExprId> findDependentInputs(ExprId outputExprId) {
    List<ExprId> dependentInputs = new LinkedList<>();
    dependentInputs.add(outputExprId);
    boolean continueSearch = true;

    Set<ExprId> newDependentInputs = new HashSet<>(Arrays.asList(outputExprId));
    while (continueSearch) {
      newDependentInputs =
          newDependentInputs.stream()
              .filter(exprId -> exprDependencies.containsKey(exprId))
              .flatMap(exprId -> exprDependencies.get(exprId).stream())
              .filter(exprId -> !dependentInputs.contains(exprId)) // filter already added
              .collect(Collectors.toSet());

      dependentInputs.addAll(newDependentInputs);
      continueSearch = !newDependentInputs.isEmpty();
    }

    return dependentInputs;
  }
}
