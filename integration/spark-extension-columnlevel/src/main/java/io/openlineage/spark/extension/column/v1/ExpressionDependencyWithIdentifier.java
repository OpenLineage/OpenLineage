/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.extension.column.v1;

import java.util.List;
import java.util.Objects;

public class ExpressionDependencyWithIdentifier implements ExpressionDependency {

  private final OlExprId outputExprId;

  private final List<OlExprId> inputExprId;

  public ExpressionDependencyWithIdentifier(OlExprId outputExprId, List<OlExprId> inputExprId) {
    this.outputExprId = outputExprId;
    this.inputExprId = inputExprId;
  }

  @Override
  public OlExprId getOutputExprId() {
    return outputExprId;
  }

  public List<OlExprId> getInputExprId() {
    return inputExprId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ExpressionDependencyWithIdentifier that = (ExpressionDependencyWithIdentifier) o;
    return Objects.equals(outputExprId, that.outputExprId)
        && Objects.equals(inputExprId, that.inputExprId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(outputExprId, inputExprId);
  }
}
