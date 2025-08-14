/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan.column;

import io.openlineage.client.utils.TransformationInfo;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.spark.sql.catalyst.expressions.ExprId;

@AllArgsConstructor
class Dependency {
  @Getter private ExprId exprId;

  @Getter private TransformationInfo transformationInfo;

  @Override
  public String toString() {
    return "Dependency("
        + exprId
        + ", "
        + transformationInfo.getType()
        + ", "
        + transformationInfo.getSubType()
        + ')';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Dependency that = (Dependency) o;
    return Objects.equals(exprId, that.exprId)
        && Objects.equals(transformationInfo, that.transformationInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(exprId, transformationInfo);
  }

  public Dependency merge(Dependency dependency) {
    if (this.transformationInfo != null) {
      TransformationInfo merged = this.transformationInfo.merge(dependency.getTransformationInfo());
      if (merged.equals(transformationInfo)) {
        // exactly the same dependency would work
        return dependency;
      } else {
        return new Dependency(dependency.exprId, merged);
      }
    }
    return new Dependency(dependency.exprId, null);
  }
}
