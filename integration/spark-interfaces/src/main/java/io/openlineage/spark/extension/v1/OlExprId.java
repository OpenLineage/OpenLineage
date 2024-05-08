/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.extension.v1;

import java.util.Objects;

/**
 * Class to contain reference to Spark's ExprId without adding dependency to Spark library
 *
 * @see <a
 *     href="https://github.com/apache/spark/blob/ce5ddad990373636e94071e7cef2f31021add07b/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/namedExpressions.scala#L48">namedExpressions.scala</a>
 */
public final class OlExprId {

  private final Long exprId;

  public OlExprId(Long exprId) {
    this.exprId = exprId;
  }

  public Long getExprId() {
    return exprId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    OlExprId olExprId = (OlExprId) o;
    return Objects.equals(exprId, olExprId.exprId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(exprId);
  }
}
