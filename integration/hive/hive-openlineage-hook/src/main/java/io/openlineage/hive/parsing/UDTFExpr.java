/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.parsing;

import java.util.List;
import lombok.Getter;
import lombok.NonNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;

@Getter
public class UDTFExpr extends BaseExpr {
  private final GenericUDTF function;

  public UDTFExpr(@NonNull GenericUDTF function, @NonNull List<BaseExpr> children) {
    super(children);
    this.function = function;
  }

  @Override
  public String toString() {
    return String.format("UDTF[%s](%s)", function, getChildren());
  }
}
