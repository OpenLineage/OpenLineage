/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.parsing;

import lombok.Getter;

@Getter
public class ConstantExpr extends BaseExpr {

  private final Object value;

  public ConstantExpr(Object value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return String.format("Constant[%s]", value);
  }
}
