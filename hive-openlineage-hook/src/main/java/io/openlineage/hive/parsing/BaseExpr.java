/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.parsing;

import java.util.List;
import lombok.Getter;

@Getter
public abstract class BaseExpr {

  private List<BaseExpr> children;

  protected BaseExpr() {}

  protected BaseExpr(List<BaseExpr> children) {
    this.children = children;
  }
}
