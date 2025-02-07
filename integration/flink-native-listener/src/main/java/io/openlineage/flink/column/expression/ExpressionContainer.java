/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.column.expression;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.ColumnLineageDatasetFacet;
import io.openlineage.client.OpenLineage.ColumnLineageDatasetFacetBuilder;
import io.openlineage.client.OpenLineage.ColumnLineageDatasetFacetFieldsAdditional;
import io.openlineage.client.OpenLineage.ColumnLineageDatasetFacetFieldsBuilder;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.InputField;
import io.openlineage.client.OpenLineage.SchemaDatasetFacet;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.flink.client.OpenLineageContext;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelNode;

/** Expression to store all the transformations and table fields. */
@Slf4j
public class ExpressionContainer {
  private final List<Expression> expressions;
  private final List<RelNode> roots;
  private final OpenLineageContext context;

  public ExpressionContainer(OpenLineageContext context, List<RelNode> roots) {
    this.expressions = new ArrayList<>();
    this.roots = roots;
    this.context = context;
  }

  public void addExpressions(List<Expression> expressions) {
    this.expressions.addAll(expressions);
  }

  public List<Expression> getOutputsOf(RelNode node) {
    // TODO: can be optimized by using a map in future
    return expressions.stream()
        .filter(e -> e.getRelNode().equals(node))
        .collect(Collectors.toList());
  }

  public ColumnLineageDatasetFacet getColumnLineage(
      List<InputDataset> inputs, SchemaDatasetFacet outputSchema) {
    Map<UUID, Expression> expressionsMap =
        expressions.stream().collect(Collectors.toMap(Expression::getUuid, e -> e));

    List<Expression> outputs =
        expressions.stream()
            .filter(e -> roots.contains(e.getRelNode()))
            .collect(Collectors.toList());

    ColumnLineageDatasetFacetBuilder builder =
        context.getOpenLineage().newColumnLineageDatasetFacetBuilder();

    ColumnLineageDatasetFacetFieldsBuilder fieldsBuilder =
        context.getOpenLineage().newColumnLineageDatasetFacetFieldsBuilder();

    outputs.forEach(
        (output) -> {
          List<TableFieldExpression> tableInputs = inputsOf(output, expressionsMap);
          log.info(
              "Output expression {} from column {} is derived from {}",
              output,
              outputSchema.getFields().get(output.getOutputRelNodeOrdinal()).getName(),
              tableInputs.stream()
                  .map(TableFieldExpression::toString)
                  .collect(Collectors.joining(",")));

          ColumnLineageDatasetFacetFieldsAdditional fieldsAdditional =
              context
                  .getOpenLineage()
                  .newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                  .inputFields(
                      tableInputs.stream()
                          .map(tf -> inputField(inputs, tf))
                          .filter(Objects::nonNull)
                          .collect(Collectors.toList()))
                  .build();

          if (!fieldsAdditional.getInputFields().isEmpty()) {
            fieldsBuilder
                .put(
                    // TODO: find out if we should rely on position;
                    // TODO: make sure TypeInformationFacetVisitor preserves ordering
                    outputSchema.getFields().get(output.getOutputRelNodeOrdinal()).getName(),
                    fieldsAdditional)
                .build();
          }
        });
    return builder.fields(fieldsBuilder.build()).build();
  }

  private InputField inputField(List<InputDataset> inputs, TableFieldExpression tableField) {
    // TODO: this matching logic needs to be tested
    List<String> tableName = tableField.getTableName();

    // get identifiers with the same table name
    List<DatasetIdentifier> di =
        inputs.stream()
            .filter(input -> input.getFacets() != null)
            .filter(input -> input.getFacets().getSymlinks() != null)
            .filter(input -> input.getFacets().getSymlinks().getIdentifiers() != null)
            .filter(input -> !input.getFacets().getSymlinks().getIdentifiers().isEmpty())
            .filter(
                input ->
                    input
                        .getFacets()
                        .getSymlinks()
                        .getIdentifiers()
                        .get(0)
                        .getName()
                        .equals(String.join(".", tableName)))
            .map(d -> new DatasetIdentifier(d.getName(), d.getNamespace()))
            .collect(Collectors.toList());

    if (di.isEmpty()) {
      log.error("Namespace not found for table field {}", tableField);
      return null;
    } else if (di.size() == 1) {
      // there exists only one dataset with the same table name
      return new OpenLineage.InputFieldBuilder()
          .name(di.get(0).getName())
          .namespace(di.get(0).getNamespace())
          .field(tableField.getName())
          .build();
    } else if (di.size() > 1) {
      // TODO: test this -> more than one input with the same name; implement better matching
      // heristics
      log.error("More than one namespace found for table field {}", tableField);
      return null;
    }

    return null;
  }

  private List<TableFieldExpression> inputsOf(
      Expression output, Map<UUID, Expression> expressionsMap) {
    // find input transformations
    Set<Expression> reachableTransformations = new HashSet<>();
    reachableTransformations.add(output);

    boolean continueSearch = true;
    while (continueSearch) {
      Set<Expression> stepExpressions =
          reachableTransformations.stream()
              .flatMap(e -> e.getInputIds().stream())
              .filter(expressionsMap::containsKey)
              .map(expressionsMap::get)
              .collect(Collectors.toSet());

      // make sure step expressions are not already in reachable transformations
      stepExpressions.removeAll(reachableTransformations);

      continueSearch = !stepExpressions.isEmpty();
      reachableTransformations.addAll(stepExpressions);
    }

    // check if any of the input transformation is a table field
    return reachableTransformations.stream()
        .filter(e -> e instanceof TableFieldExpression)
        .map(TableFieldExpression.class::cast)
        .collect(Collectors.toList());
  }
}
