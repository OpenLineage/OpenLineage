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
