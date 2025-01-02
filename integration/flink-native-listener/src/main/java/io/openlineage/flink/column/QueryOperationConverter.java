/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.column;

import io.openlineage.client.OpenLineage.ColumnLineageDatasetFacet;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.SchemaDatasetFacet;
import io.openlineage.flink.client.OpenLineageContext;
import io.openlineage.flink.column.expression.Expression;
import io.openlineage.flink.column.expression.ExpressionContainer;
import io.openlineage.flink.column.visitors.AggregateVisitor;
import io.openlineage.flink.column.visitors.CalciteNodeVisitor;
import io.openlineage.flink.column.visitors.ProjectVisitor;
import io.openlineage.flink.column.visitors.TableScanVisitor;
import io.openlineage.flink.column.visitors.WatermarkAssignerVisitor;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;

@Slf4j
public class QueryOperationConverter {

  private final List<CalciteNodeVisitor> visitors;
  private final OpenLineageContext context;

  public QueryOperationConverter(OpenLineageContext context) {
    this.context = context;
    visitors =
        Arrays.asList(
            new ProjectVisitor(),
            new TableScanVisitor(),
            new WatermarkAssignerVisitor(),
            new AggregateVisitor());
  }

  /** Extract column level lineage from the RelNode. */
  public Optional<ColumnLineageDatasetFacet> convert(
      List<InputDataset> inputs,
      SchemaDatasetFacet schemaDatasetFacet,
      List<QueryOperation> queryOperations) {
    // visit the relNode
    if (!(queryOperations.get(0) instanceof PlannerQueryOperation)) {
      return Optional.empty();
    }

    RelNode outputNode = ((PlannerQueryOperation) queryOperations.get(0)).getCalciteTree();
    List<LineageRelNode> nodesToVisit = LineageRelNode.loadTree(outputNode);

    // sort nodesToVisit by depth to ensure that we visit the child before the parent
    nodesToVisit.sort(Comparator.comparingInt(LineageRelNode::getDepth).reversed());
    ExpressionContainer expressionContainer = loadTransformations(nodesToVisit);

    return Optional.of(expressionContainer.getColumnLineage(inputs, schemaDatasetFacet));
  }

  private ExpressionContainer loadTransformations(List<LineageRelNode> nodesToVisit) {
    ExpressionContainer expressionContainer =
        new ExpressionContainer(
            context,
            nodesToVisit.stream()
                .filter(node -> node.getDepth() == 0)
                .map(LineageRelNode::getRelNode)
                .collect(Collectors.toList()));

    nodesToVisit.forEach(
        node ->
            visitors.stream()
                .filter(visitor -> visitor.isDefinedAt(node.getRelNode()))
                .forEach(
                    v -> {
                      List<Expression> expressions =
                          v.loadExpressions(node.getRelNode(), expressionContainer);
                      expressionContainer.addExpressions(expressions);
                      node.addOutputTransformations(expressions);
                    }));

    return expressionContainer;
  }
  // TODO: support multiple relNodes in a query operation
}
