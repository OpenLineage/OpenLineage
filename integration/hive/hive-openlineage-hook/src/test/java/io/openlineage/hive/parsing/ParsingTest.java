/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.parsing;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.hive.InMemoryHiveTestBase;
import io.openlineage.hive.util.HiveUtils;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFSum;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFConcat;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLag;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;

public class ParsingTest extends InMemoryHiveTestBase {

  @Test
  void testSelectStar() throws TException {
    createTable("t1", "a;int", "b;string");
    String queryString = "CREATE TABLE t2 AS SELECT * FROM t1";
    SemanticAnalyzer semanticAnalyzer = HiveUtils.analyzeQuery(hiveConf, queryState, queryString);
    QueryExpr queryExpr = Parsing.buildQueryTree(semanticAnalyzer.getQB(), "default.t2");
    assertThat(queryExpr.getId()).isNull();
    assertThat(queryExpr.getSelectAliases()).containsExactly("a", "b");
    assertThat(queryExpr.getSubQueries().keySet()).containsExactly("t1");
    QueryExpr subQuery = queryExpr.getSubQueries().get("t1").get(0);
    assertThat(subQuery.getSelectAliases()).containsExactly("a", "b");
  }

  @Test
  void testJoin() throws TException {
    createTable("t1", "a;int", "b;string");
    createTable("t2", "c;int", "d;string");
    String queryString = "CREATE TABLE t3 AS SELECT t1.a, t2.d FROM t1 JOIN t2 ON t1.a = t2.c";
    SemanticAnalyzer semanticAnalyzer = HiveUtils.analyzeQuery(hiveConf, queryState, queryString);
    QueryExpr queryExpr = Parsing.buildQueryTree(semanticAnalyzer.getQB(), "default.t3");
    assertThat(queryExpr.getId()).isNull();
    assertThat(queryExpr.getSelectAliases()).containsExactly("a", "d");
    assertThat(queryExpr.getSubQueries().keySet()).containsExactlyInAnyOrder("t1", "t2");
    assertThat(queryExpr.getJoinExpressions()).hasSize(2);

    // Left side of the JOIN
    ColumnExpr leftSide = (ColumnExpr) queryExpr.getJoinExpressions().get(0);
    assertThat(leftSide.getName()).isEqualTo("a");
    assertThat(leftSide.getQueries().get(0).getId()).isEqualTo("t1");

    // Right side of the JOIN
    ColumnExpr rightSide = (ColumnExpr) queryExpr.getJoinExpressions().get(1);
    assertThat(rightSide.getName()).isEqualTo("c");
    assertThat(rightSide.getQueries().get(0).getId()).isEqualTo("t2");
  }

  @Test
  void testGroupBy() throws TException {
    createTable("t1", "a;int", "b;string");
    String queryString = "CREATE TABLE t2 AS SELECT a, COUNT(*) FROM t1 GROUP BY a";
    SemanticAnalyzer semanticAnalyzer = HiveUtils.analyzeQuery(hiveConf, queryState, queryString);
    QueryExpr queryExpr = Parsing.buildQueryTree(semanticAnalyzer.getQB(), "default.t2");
    assertThat(queryExpr.getId()).isNull();
    assertThat(queryExpr.getSelectAliases()).containsExactly("a", "_1");
    assertThat(queryExpr.getSubQueries().keySet()).containsExactlyInAnyOrder("t1");
    assertThat(queryExpr.getGroupByExpressions()).hasSize(1);

    // Group by column
    ColumnExpr groupByColumn = (ColumnExpr) queryExpr.getGroupByExpressions().get(0);
    assertThat(groupByColumn.getName()).isEqualTo("a");
    assertThat(groupByColumn.getQueries().get(0).getId()).isEqualTo("t1");
  }

  @Test
  void testFilter() throws TException {
    createTable("t1", "a;int", "b;string");
    String queryString = "CREATE TABLE t2 AS SELECT * FROM t1 WHERE a > 10";
    SemanticAnalyzer semanticAnalyzer = HiveUtils.analyzeQuery(hiveConf, queryState, queryString);
    QueryExpr queryExpr = Parsing.buildQueryTree(semanticAnalyzer.getQB(), "default.t2");
    assertThat(queryExpr.getId()).isNull();
    assertThat(queryExpr.getSelectAliases()).containsExactly("a", "b");
    assertThat(queryExpr.getSubQueries().keySet()).containsExactlyInAnyOrder("t1");
    assertThat(queryExpr.getWhereExpressions()).hasSize(1);

    // Filter expression
    GenericExpr filterExpr = (GenericExpr) queryExpr.getWhereExpressions().get(0);
    assertThat(filterExpr.getChildren()).hasSize(2); // Should have two children: column and value

    // Verify the column used in the filter (t1.a)
    ColumnExpr columnExpr = (ColumnExpr) filterExpr.getChildren().get(0);
    assertThat(columnExpr.getName()).isEqualTo("a");
    assertThat(columnExpr.getQueries().get(0).getId()).isEqualTo("t1");
  }

  @Test
  void testFunction() throws TException {
    createTable("t1", "a;int", "b;string");
    String queryString = "CREATE TABLE t2 AS SELECT a, CONCAT(b, 'xyz') AS mycol FROM t1";
    SemanticAnalyzer semanticAnalyzer = HiveUtils.analyzeQuery(hiveConf, queryState, queryString);
    QueryExpr queryExpr = Parsing.buildQueryTree(semanticAnalyzer.getQB(), "default.t2");
    assertThat(queryExpr.getId()).isNull();
    assertThat(queryExpr.getSelectAliases()).containsExactly("a", "mycol");
    assertThat(queryExpr.getSubQueries().keySet()).containsExactlyInAnyOrder("t1");
    assertThat(queryExpr.getSelectExpressions()).hasSize(2);

    // Check the function
    FunctionExpr functionExpr = (FunctionExpr) queryExpr.getSelectExpressions().get(1);
    assertThat(functionExpr.getFunction()).isInstanceOf(GenericUDFConcat.class);
    ColumnExpr columnExpr = (ColumnExpr) functionExpr.getChildren().get(0);
    assertThat(columnExpr.getName()).isEqualTo("b");
    assertThat(columnExpr.getQueries().get(0).getId()).isEqualTo("t1");
  }

  @Test
  void testAggregation() throws TException {
    createTable("t1", "a;int", "b;string");
    String queryString = "CREATE TABLE t2 AS SELECT b, sum(a) AS sum_a FROM t1 GROUP BY b";
    SemanticAnalyzer semanticAnalyzer = HiveUtils.analyzeQuery(hiveConf, queryState, queryString);
    QueryExpr queryExpr = Parsing.buildQueryTree(semanticAnalyzer.getQB(), "default.t2");
    assertThat(queryExpr.getId()).isNull();
    assertThat(queryExpr.getSelectAliases()).containsExactly("b", "sum_a");
    assertThat(queryExpr.getSubQueries().keySet()).containsExactlyInAnyOrder("t1");
    assertThat(queryExpr.getSelectExpressions()).hasSize(2);

    // Check the aggregation function
    AggregateExpr aggregateExpr = (AggregateExpr) queryExpr.getSelectExpressions().get(1);
    assertThat(aggregateExpr.getFunction()).isInstanceOf(GenericUDAFSum.class);
    ColumnExpr columnExpr = (ColumnExpr) aggregateExpr.getChildren().get(0);
    assertThat(columnExpr.getName()).isEqualTo("a");
    assertThat(columnExpr.getQueries().get(0).getId()).isEqualTo("t1");
  }

  @Test
  void testWindow() throws TException {
    createTable("t1", "a;int", "b;string", "c;int");
    String queryString =
        "CREATE TABLE t2 AS SELECT a, LAG(b, 1) OVER (PARTITION BY a ORDER BY c) AS lagged_b FROM t1";
    SemanticAnalyzer semanticAnalyzer = HiveUtils.analyzeQuery(hiveConf, queryState, queryString);
    QueryExpr queryExpr = Parsing.buildQueryTree(semanticAnalyzer.getQB(), "default.t2");
    assertThat(queryExpr.getId()).isNull();
    assertThat(queryExpr.getSelectAliases()).containsExactly("a", "lagged_b");
    assertThat(queryExpr.getSubQueries().keySet()).containsExactlyInAnyOrder("t1");
    assertThat(queryExpr.getSelectExpressions()).hasSize(2);

    // Check the function
    FunctionExpr functionExpr = (FunctionExpr) queryExpr.getSelectExpressions().get(1);
    assertThat(functionExpr.getFunction()).isInstanceOf(GenericUDFLag.class);
    ColumnExpr functionColumn = (ColumnExpr) functionExpr.getChildren().get(0);
    assertThat(functionColumn.getName()).isEqualTo("b");
    assertThat(functionColumn.getQueries().get(0).getId()).isEqualTo("t1");

    // Verify the window expression
    WindowExpr windowExpr =
        (WindowExpr) functionExpr.getChildren().get(functionExpr.getChildren().size() - 1);
    ColumnExpr partitionColumn =
        (ColumnExpr) windowExpr.getChildren().get(0).getChildren().get(0).getChildren().get(0);
    assertThat(partitionColumn.getName()).isEqualTo("a");
    assertThat(partitionColumn.getQueries().get(0).getId()).isEqualTo("t1");
    ColumnExpr orderColumn =
        (ColumnExpr)
            windowExpr
                .getChildren()
                .get(0)
                .getChildren()
                .get(1)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0);
    assertThat(orderColumn.getName()).isEqualTo("c");
    assertThat(orderColumn.getQueries().get(0).getId()).isEqualTo("t1");
  }
}
