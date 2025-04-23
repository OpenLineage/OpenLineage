/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.parsing;

import java.util.List;
import lombok.Getter;
import lombok.NonNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;

@Getter
public class FunctionExpr extends BaseExpr {
  private final GenericUDF function;
  private final String name;

  public FunctionExpr(@NonNull String name, GenericUDF function, @NonNull List<BaseExpr> children) {
    super(children);
    this.name = name;
    this.function = function;
  }

  @Override
  public String toString() {
    return String.format("Function[%s](%s)", name, getChildren());
  }
}
