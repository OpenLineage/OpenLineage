/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan.column;

import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.spark.sql.catalyst.expressions.ExprId;

@AllArgsConstructor
public class DatasetDependency {
  @Getter private ExprId exprId;
  @Getter private String outputExpression;
  @Getter private String description;

  @Override
  public String toString() {
    return "DatasetDependency{"
        + "exprId="
        + exprId
        + ", outputExpression='"
        + outputExpression
        + '\''
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    DatasetDependency that = (DatasetDependency) o;
    return Objects.equals(exprId, that.exprId)
        && Objects.equals(outputExpression, that.outputExpression);
  }

  @Override
  public int hashCode() {
    return Objects.hash(exprId, outputExpression);
  }
}
