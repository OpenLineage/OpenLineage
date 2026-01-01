/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.parsing;

import static io.openlineage.hive.parsing.ParsingTestUtils.createGreaterThanExpr;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class QueryExprTest {

  @Test
  void testQueryExpr() {
    String queryId = "query_001";
    QueryExpr queryExpr = new QueryExpr(queryId);
    assertThat(queryExpr.getId()).isEqualTo(queryId);
    BaseExpr selectExpr = new ColumnExpr("column1");
    queryExpr.addSelectExpression("alias1", selectExpr);
    assertThat(queryExpr.getSelectAliases()).containsExactly("alias1");
    assertThat(queryExpr.getSelectExpressions()).containsExactly(selectExpr);
    List<String> aliases = asList("alias2", "alias3");
    List<BaseExpr> exprs =
        asList(new ConstantExpr("5"), createGreaterThanExpr("column2", "column3"));
    queryExpr.addSelectExpressions(aliases, exprs);
    assertThat(queryExpr.getSelectAliases()).containsExactly("alias1", "alias2", "alias3");
    assertThat(queryExpr.getSelectExpressions())
        .containsExactly(selectExpr, exprs.get(0), exprs.get(1));
  }
}
