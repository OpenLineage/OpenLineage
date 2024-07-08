/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.extension.column.v1;

import java.util.Objects;

public class OutputDatasetField implements DatasetFieldLineage {

  private final OlExprId exprId;

  private final String field;

  public OutputDatasetField(OlExprId exprId, String field) {
    this.exprId = exprId;
    this.field = field;
  }

  public OlExprId getExprId() {
    return exprId;
  }

  @Override
  public String getField() {
    return field;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    OutputDatasetField that = (OutputDatasetField) o;
    return Objects.equals(exprId, that.exprId) && Objects.equals(field, that.field);
  }

  @Override
  public int hashCode() {
    return Objects.hash(exprId, field);
  }
}
