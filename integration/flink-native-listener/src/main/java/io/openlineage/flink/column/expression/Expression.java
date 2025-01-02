/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.column.expression;

import java.util.UUID;
import org.apache.calcite.rel.RelNode;

/**
 * Expression is a representation of a column transformation.
 */
public interface Expression {

  /**
   * Auto generated expression identifier.
   *
   * @return UUID
   */
  UUID getUuid();

  /**
   * The input RelNode of the transformation.
   *
   * @return
   */
  RelNode getInputRelNode();

  /**
   * RelNode often describe their output as a list of expressions. For example, a Project node
   * uses index ouf the input RelNode to define output field.
   *
   * @return
   */
  int getOutputRelNodeOrdinal();
}
