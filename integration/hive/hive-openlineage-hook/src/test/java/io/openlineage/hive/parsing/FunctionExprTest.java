/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.parsing;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPPlus;
import org.junit.jupiter.api.Test;

class FunctionExprTest {

  @Test
  void testFunction() {
    GenericUDFOPPlus plusUDF = new GenericUDFOPPlus();
    List<BaseExpr> children = Arrays.asList(new ColumnExpr("x"), new ConstantExpr("5"));
    FunctionExpr functionExpr = new FunctionExpr("plus", plusUDF, children);
    assertThat(functionExpr.getFunction()).isEqualTo(plusUDF);
    assertThat(functionExpr.getChildren()).isEqualTo(children);
    String expected = "Function[plus]([Column[x], Constant[5]])";
    assertThat(functionExpr.toString()).isEqualTo(expected);
  }
}
