/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.column.expression;

import java.util.List;
import java.util.UUID;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;

@Getter
@Builder
@EqualsAndHashCode
@ToString
public class TransformationExpression implements Expression {
  private final UUID uuid;
  private final RelNode relNode;
  private final int outputRelNodeOrdinal;
  private final List<UUID> inputIds;

  private final String transformation;
  private final String outputName;
  private final RexNode rexNode;
}
