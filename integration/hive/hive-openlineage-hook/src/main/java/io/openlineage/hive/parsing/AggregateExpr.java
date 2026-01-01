/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.parsing;

import java.util.List;
import lombok.Getter;
import lombok.NonNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver;

@SuppressWarnings("deprecation")
@Getter
public class AggregateExpr extends BaseExpr {
  private final GenericUDAFResolver function;

  public AggregateExpr(@NonNull GenericUDAFResolver function, @NonNull List<BaseExpr> children) {
    super(children);
    this.function = function;
  }

  @Override
  public String toString() {
    return String.format("Aggregation[%s](%s)", function, getChildren());
  }
}
