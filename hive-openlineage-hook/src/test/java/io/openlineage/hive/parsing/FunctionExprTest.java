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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPPlus;
import org.junit.jupiter.api.Test;

class FunctionExprTest {

  @Test
  void testFunction() {
    GenericUDFOPPlus plusUDF = new GenericUDFOPPlus();
    List<BaseExpr> children = Arrays.asList(new ConstantExpr("x"), new ConstantExpr("5"));
    FunctionExpr functionExpr = new FunctionExpr(plusUDF, children);
    assertThat(functionExpr.getFunction()).isEqualTo(plusUDF);
    assertThat(functionExpr.getChildren()).isEqualTo(children);
    String expected = "Function (GenericUDFOPPlus): [Column[x], Const string 5]";
    assertThat(functionExpr.toString()).isEqualTo(expected);
  }
}
