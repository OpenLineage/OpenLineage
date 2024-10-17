/*
 * Copyright 2024 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openlineage.hive.parsing;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;

public class ParsingTestUtils {

  public static BaseExpr createParsedExpr(String columnName) {
    ColumnExpr column = new ColumnExpr();
    column.setName(columnName);
    return column;
  }

  public static BaseExpr createConstantExpr(String value) {
    return new ConstantExpr(value);
  }

  public static BaseExpr createGreaterThanExpr(String leftCol, String rightCol) {
    List<BaseExpr> children = Arrays.asList(createParsedExpr(leftCol), createParsedExpr(rightCol));
    return new FunctionExpr(new GenericUDFOPGreaterThan(), children);
  }
}
