/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.parsing;

import java.util.List;
import lombok.Getter;
import lombok.NonNull;

@Getter
public class IfExpr extends BaseExpr {

  public IfExpr(@NonNull List<BaseExpr> children) {
    super(children);
  }

  @Override
  public String toString() {
    return String.format(
        "If: [%s, %s, %s]", getChildren().get(0), getChildren().get(1), getChildren().get(2));
  }
}
