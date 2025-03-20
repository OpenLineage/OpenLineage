/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.parsing;

import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;

public class ParsingTestUtils {

  public static BaseExpr createGreaterThanExpr(String leftCol, String rightCol) {
    List<BaseExpr> children = Arrays.asList(new ColumnExpr(leftCol), new ColumnExpr(rightCol));
    return new FunctionExpr("greater_than", new GenericUDFOPGreaterThan(), children);
  }
}
