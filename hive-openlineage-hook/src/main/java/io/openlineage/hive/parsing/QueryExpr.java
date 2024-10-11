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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;

@Getter
@RequiredArgsConstructor
public class QueryExpr extends ExprNodeDesc {
  private static final long serialVersionUID = 1L;

  private final String id;
  private final Map<String, List<QueryExpr>> subQueries = new HashMap<>();
  private final List<ExprNodeDesc> selectExpressions = new ArrayList<>();
  private final List<String> selectAliases = new ArrayList<>();
  private final List<ExprNodeDesc> groupByExpressions = new ArrayList<>();
  private final List<ExprNodeDesc> whereExpressions = new ArrayList<>();
  private final List<ExprNodeDesc> joinExpressions = new ArrayList<>();
  private final List<ExprNodeDesc> orderByExpressions = new ArrayList<>();

  public void addSelectExpression(String alias, ExprNodeDesc expr) {
    selectAliases.add(alias);
    selectExpressions.add(expr);
  }

  public void addSelectExpressions(List<String> aliases, List<ExprNodeDesc> exprs) {
    selectAliases.addAll(aliases);
    selectExpressions.addAll(exprs);
  }

  @Override
  @SuppressWarnings("PMD.CloneMethodMustImplementCloneable")
  public QueryExpr clone() {
    // Not implemented because not needed for our purposes
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isSame(Object o) {
    // Not implemented because not needed for our purposes
    throw new UnsupportedOperationException();
  }
}
