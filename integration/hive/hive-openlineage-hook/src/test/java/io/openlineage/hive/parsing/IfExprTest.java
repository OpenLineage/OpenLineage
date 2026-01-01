/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.parsing;

import static io.openlineage.hive.parsing.ParsingTestUtils.createGreaterThanExpr;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class IfExprTest {

  @Test
  void testIf() {
    List<BaseExpr> children =
        Arrays.asList(
            createGreaterThanExpr("x", "y"),
            new ConstantExpr("greater value"),
            new ConstantExpr("lesser or equal value"));
    IfExpr ifExpr = new IfExpr(children);
    String expected =
        "If: [Function[greater_than]([Column[x], Column[y]]), Constant[greater value], Constant[lesser or equal value]]";
    assertThat(ifExpr.toString()).isEqualTo(expected);
  }
}
