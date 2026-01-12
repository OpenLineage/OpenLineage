/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.shade.extension.v1;

import java.util.Objects;

/**
 * A class to hold a reference to Spark's {@code ExprId} without introducing a dependency on the
 * Spark library.
 *
 * <p>This class serves as a lightweight alternative for storing {@code ExprId}, which is used in
 * Spark's expression identifiers, while avoiding direct integration with Spark's internal
 * libraries.
 *
 * @see <a
 *     href="https://github.com/apache/spark/blob/ce5ddad990373636e94071e7cef2f31021add07b/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/namedExpressions.scala#L48">namedExpressions.scala</a>
 */
public final class OlExprId {

  private final Long exprId;

  /**
   * Constructs a new {@code OlExprId} with the specified expression identifier.
   *
   * @param exprId the expression identifier
   */
  public OlExprId(Long exprId) {
    this.exprId = exprId;
  }

  /**
   * Returns the expression identifier.
   *
   * @return the expression identifier as a {@link Long}
   */
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
