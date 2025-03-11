/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.parsing;

import java.util.List;
import lombok.Getter;
import lombok.NonNull;

@Getter
public class WindowExpr extends BaseExpr {

  public WindowExpr(@NonNull List<BaseExpr> children) {
    super(children);
  }

  @Override
  public String toString() {
    return String.format("Window(%s)", getChildren());
  }
}
