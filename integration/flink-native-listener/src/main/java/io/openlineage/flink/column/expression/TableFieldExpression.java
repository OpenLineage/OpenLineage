/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.column.expression;

import java.util.List;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rel.RelNode;


@Getter
@AllArgsConstructor
@Builder
public class TableFieldExpression implements Expression {

  private final RexNode rexNode;
  private final RelNode inputRelNode;
  private final int outputRelNodeOrdinal;

  private final String name;
  private final List<String> tableName;
  private final UUID uuid;
}
