/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.parsing;

import java.util.List;
import lombok.Getter;
import lombok.NonNull;

@Getter
public class CaseWhenExpr extends BaseExpr {

  public CaseWhenExpr(@NonNull List<BaseExpr> children) {
    super(children);
  }

  @Override
  public String toString() {
    return String.format("CaseWhen: %s", getChildren());
  }
}
