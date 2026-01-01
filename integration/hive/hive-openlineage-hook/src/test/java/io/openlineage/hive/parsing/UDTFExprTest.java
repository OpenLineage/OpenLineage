/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.parsing;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTFExplode;
import org.junit.jupiter.api.Test;

class UDTFExprTest {

  @Test
  void testUDTFExpr() {
    GenericUDTF function = new GenericUDTFExplode();
    List<BaseExpr> children = asList(new ColumnExpr("array_column"), new ConstantExpr("5"));
    UDTFExpr udtfExpr = new UDTFExpr(function, children);
    assertThat(udtfExpr.getFunction()).isEqualTo(function);
    assertThat(udtfExpr.getChildren()).isEqualTo(children);
    assertThat(udtfExpr.toString()).isEqualTo("UDTF[explode]([Column[array_column], Constant[5]])");
  }
}
