/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.column;

import io.openlineage.flink.column.expression.Expression;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;

@Getter
@AllArgsConstructor
public class LineageRelNode {
  private final LineageRelNode parent;
  private final RelNode relNode;
  private final int depth; // depth of the node in the lineage tree. 0 for output RelNode.
  private final List<Expression> outputTransformations = new ArrayList<>();

  public void addOutputTransformations(List<Expression> transformationExpression) {
    outputTransformations.addAll(transformationExpression);
  }

  public static List<LineageRelNode> loadTree(RelNode calciteTree) {
    LineageRelNode root = new LineageRelNode(null, calciteTree, 0);

    List<LineageRelNode> nodes = loadTree(root, calciteTree.getInputs(), 1);
    nodes.add(root);

    return nodes;
  }

  private static List<LineageRelNode> loadTree(
      LineageRelNode parent, List<RelNode> nodes, int depth) {
    List<LineageRelNode> resultNodes = new ArrayList<>();
    for (RelNode relNode : nodes) {
      LineageRelNode lineageRelNode = new LineageRelNode(parent, relNode, depth);
      resultNodes.add(lineageRelNode);
      resultNodes.addAll(loadTree(lineageRelNode, relNode.getInputs(), depth + 1));
    }
    return resultNodes;
  }
}
