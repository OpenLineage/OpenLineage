/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
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
