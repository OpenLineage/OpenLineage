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

@Getter
@RequiredArgsConstructor
public class QueryExpr extends BaseExpr {

  private final String id;
  private final Map<String, List<QueryExpr>> subQueries = new HashMap<>();
  private final List<BaseExpr> selectExpressions = new ArrayList<>();
  private final List<String> selectAliases = new ArrayList<>();
  private final List<BaseExpr> groupByExpressions = new ArrayList<>();
  private final List<BaseExpr> whereExpressions = new ArrayList<>();
  private final List<BaseExpr> joinExpressions = new ArrayList<>();
  private final List<BaseExpr> orderByExpressions = new ArrayList<>();

  public void addSelectExpression(String alias, BaseExpr expr) {
    selectAliases.add(alias);
    selectExpressions.add(expr);
  }

  public void addSelectExpressions(List<String> aliases, List<BaseExpr> exprs) {
    selectAliases.addAll(aliases);
    selectExpressions.addAll(exprs);
  }
}
