/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.column;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

@Slf4j
public class RexUtils {

  public static List<Integer> getRexDependencies(RexNode node) {
    if (node instanceof RexInputRef) {
      return Collections.singletonList(((RexInputRef)node).getIndex());
    } else if (node instanceof RexCall) {
      return ((RexCall)node)
          .getOperands()
          .stream()
          .flatMap(op -> RexUtils.getRexDependencies(op).stream())
          .collect(Collectors.toList());
    } else if (node instanceof RexLiteral) {
      return Collections.emptyList();
    } else {
      log.warn("Unsupported rex node type: {}", node);
      return Collections.emptyList();

      // TODO: refactor instead of extending the if-else chain
    }
  }

}
