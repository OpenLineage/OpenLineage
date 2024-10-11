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
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

public class ParsingTestUtils {

  public static ExprNodeDesc createExprNodeDesc(String columnName) {
    return new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, columnName, "table", false);
  }

  public static ExprNodeDesc createConstantExprNodeDesc(String value) {
    return new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, value);
  }

  public static ExprNodeDesc createGreaterThanExpr(String leftCol, String rightCol) {
    List<ExprNodeDesc> children =
        Arrays.asList(createExprNodeDesc(leftCol), createExprNodeDesc(rightCol));
    return new ExprNodeGenericFuncDesc(
        TypeInfoFactory.booleanTypeInfo, new GenericUDFOPGreaterThan(), children);
  }
}
