/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.parsing;

import static io.openlineage.hive.hooks.TransformationInfo.Subtypes.CONDITIONAL;
import static io.openlineage.hive.hooks.TransformationInfo.Subtypes.FILTER;
import static io.openlineage.hive.hooks.TransformationInfo.Subtypes.GROUP_BY;
import static io.openlineage.hive.hooks.TransformationInfo.Subtypes.JOIN;
import static io.openlineage.hive.hooks.TransformationInfo.Subtypes.SORT;
import static io.openlineage.hive.hooks.TransformationInfo.Subtypes.WINDOW;
import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.hive.InMemoryHiveTestBase;
import io.openlineage.hive.hooks.OutputCLL;
import io.openlineage.hive.hooks.TransformationInfo;
import java.util.Arrays;
import java.util.HashSet;
import org.apache.hadoop.hive.ql.udf.UDFAcos;
import org.apache.hadoop.hive.ql.udf.UDFCrc32;
import org.apache.hadoop.hive.ql.udf.UDFMd5;
import org.apache.hadoop.hive.ql.udf.UDFSha1;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCount;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFCeil;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFMaskHash;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFMurmurHash;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFSha2;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;

public class ColumnLineageCollectorTest extends InMemoryHiveTestBase {

  @Test
  void masking() {
    // Test the masking functions
    assertThat(ColumnLineageCollector.isMasking(UDFCrc32.class)).isTrue();
    assertThat(ColumnLineageCollector.isMasking(UDFMd5.class)).isTrue();
    assertThat(ColumnLineageCollector.isMasking(GenericUDFMurmurHash.class)).isTrue();
    assertThat(ColumnLineageCollector.isMasking(UDFSha1.class)).isTrue();
    assertThat(ColumnLineageCollector.isMasking(GenericUDFSha2.class)).isTrue();
    assertThat(ColumnLineageCollector.isMasking(GenericUDFMaskHash.class)).isTrue();
    assertThat(ColumnLineageCollector.isMasking(GenericUDAFCount.class)).isTrue();

    // Test some non-masking functions
    assertThat(ColumnLineageCollector.isMasking(UDFAcos.class)).isFalse();
    assertThat(ColumnLineageCollector.isMasking(GenericUDFCeil.class)).isFalse();
  }

  @Test
  void selectStar() throws TException {
    createTable("t1", "a;int", "b;string");
    String queryString = "CREATE TABLE xxx AS SELECT * FROM t1";
    OutputCLL outputCLL = getOutputCLL(queryString, "default.xxx", "a;int", "b;string");
    assertThat(outputCLL.getInputTables().keySet()).containsExactlyInAnyOrder("default.t1");
    assertCountColumnDependencies(outputCLL, 2);
    assertThat(outputCLL.getColumnDependencies().get("a").get("default.t1.a"))
        .containsExactlyInAnyOrder(TransformationInfo.identity());
    assertThat(outputCLL.getColumnDependencies().get("b").get("default.t1.b"))
        .containsExactlyInAnyOrder(TransformationInfo.identity());
  }

  @Test
  void simpleQueryOnlyIdentity() throws TException {
    createTable("t1", "a;int");
    String queryString = "CREATE TABLE IF NOT EXISTS xxx AS SELECT a FROM t1";
    OutputCLL outputCLL = getOutputCLL(queryString, "default.xxx", "a;int");
    assertThat(outputCLL.getInputTables().keySet()).containsExactlyInAnyOrder("default.t1");
    assertCountColumnDependencies(outputCLL, 1);
    assertThat(outputCLL.getColumnDependencies().get("a").get("default.t1.a"))
        .containsExactlyInAnyOrder(TransformationInfo.identity());
    assertCountDatasetDependencies(outputCLL, 0);
  }

  @Test
  void ifNotExists() throws TException {
    createTable("t1", "a;int");
    String queryString = "CREATE TABLE IF NOT EXISTS xxx AS SELECT a FROM t1";
    OutputCLL outputCLL = getOutputCLL(queryString, "default.xxx", "a;int");
    assertThat(outputCLL.getInputTables().keySet()).containsExactlyInAnyOrder("default.t1");
    assertCountColumnDependencies(outputCLL, 1);
    assertThat(outputCLL.getColumnDependencies().get("a").get("default.t1.a"))
        .containsExactlyInAnyOrder(TransformationInfo.identity());
    assertCountDatasetDependencies(outputCLL, 0);
  }

  @Test
  void partitionedTable() throws TException {
    createPartitionedTable("t1", "b;string", "a;int");
    String queryString = "CREATE TABLE xxx AS SELECT * FROM t1";
    OutputCLL outputCLL = getOutputCLL(queryString, "default.xxx", "a;int", "b;string");
    assertThat(outputCLL.getInputTables().keySet()).containsExactlyInAnyOrder("default.t1");
    assertCountColumnDependencies(outputCLL, 2);
    assertThat(outputCLL.getColumnDependencies().get("a").get("default.t1.a"))
        .containsExactlyInAnyOrder(TransformationInfo.identity());
    assertThat(outputCLL.getColumnDependencies().get("b").get("default.t1.b"))
        .containsExactlyInAnyOrder(TransformationInfo.identity());
    assertCountDatasetDependencies(outputCLL, 0);
  }

  @Test
  void simpleQueryOnlyTransform() throws TException {
    createTable("t1", "a;int", "b;int");
    String queryString = "CREATE TABLE xxx AS SELECT concat(a, 'test') AS a, a+b as b FROM t1";
    OutputCLL outputCLL = getOutputCLL(queryString, "default.xxx", "a;int", "b;int");
    assertThat(outputCLL.getInputTables().keySet()).containsExactlyInAnyOrder("default.t1");
    assertCountColumnDependencies(outputCLL, 3);
    assertThat(outputCLL.getColumnDependencies().get("a").get("default.t1.a"))
        .containsExactlyInAnyOrder(TransformationInfo.transformation());
    assertThat(outputCLL.getColumnDependencies().get("b").get("default.t1.a"))
        .containsExactlyInAnyOrder(TransformationInfo.transformation());
    assertThat(outputCLL.getColumnDependencies().get("b").get("default.t1.b"))
        .containsExactlyInAnyOrder(TransformationInfo.transformation());
    assertCountDatasetDependencies(outputCLL, 0);
  }

  @Test
  void simpleQueryOnlyAggregation() throws TException {
    createTable("t1", "a;int");
    String queryString = "CREATE TABLE xxx AS SELECT count(a) AS a FROM t1";
    OutputCLL outputCLL = getOutputCLL(queryString, "default.xxx", "a;int");
    assertThat(outputCLL.getInputTables().keySet()).containsExactlyInAnyOrder("default.t1");
    assertCountColumnDependencies(outputCLL, 1);
    assertThat(outputCLL.getColumnDependencies().get("a").get("default.t1.a"))
        .containsExactlyInAnyOrder(TransformationInfo.aggregation(true));
    assertCountDatasetDependencies(outputCLL, 0);
  }

  @Test
  void simpleQueryIndirect() throws TException {
    createTable("t1", "a;int", "b;int");
    String queryString = "CREATE TABLE xxx AS SELECT a FROM t1 WHERE b > 1";
    OutputCLL outputCLL = getOutputCLL(queryString, "default.xxx", "a;int");
    assertThat(outputCLL.getInputTables().keySet()).containsExactlyInAnyOrder("default.t1");
    assertCountColumnDependencies(outputCLL, 1);
    assertThat(outputCLL.getColumnDependencies().get("a").get("default.t1.a"))
        .containsExactlyInAnyOrder(TransformationInfo.identity());
    assertCountDatasetDependencies(outputCLL, 1);
    assertThat(outputCLL.getDatasetDependencies().get("default.t1.b"))
        .containsExactlyInAnyOrder(TransformationInfo.indirect(FILTER));
  }

  @Test
  void simpleQueryMultipleIndirect() throws TException {
    createTable("t1", "a;int", "b;int", "c;int");
    String queryString =
        "CREATE TABLE xxx AS SELECT a, c FROM t1 WHERE b > 1 GROUP BY a, c ORDER BY c";
    OutputCLL outputCLL = getOutputCLL(queryString, "default.xxx", "a;int", "c;int");
    assertThat(outputCLL.getInputTables().keySet()).containsExactlyInAnyOrder("default.t1");
    assertCountColumnDependencies(outputCLL, 2);
    assertThat(outputCLL.getColumnDependencies().get("a").get("default.t1.a"))
        .containsExactlyInAnyOrder(TransformationInfo.identity());
    assertThat(outputCLL.getColumnDependencies().get("c").get("default.t1.c"))
        .containsExactlyInAnyOrder(TransformationInfo.identity());
    assertCountDatasetDependencies(outputCLL, 4);
    assertThat(outputCLL.getDatasetDependencies().get("default.t1.a"))
        .containsExactlyInAnyOrder(TransformationInfo.indirect(GROUP_BY));
    assertThat(outputCLL.getDatasetDependencies().get("default.t1.b"))
        .containsExactlyInAnyOrder(TransformationInfo.indirect(FILTER));
    assertThat(outputCLL.getDatasetDependencies().get("default.t1.c"))
        .isEqualTo(
            new HashSet<>(
                Arrays.asList(
                    TransformationInfo.indirect(GROUP_BY), TransformationInfo.indirect(SORT))));
  }

  @Test
  void simpleQueryPriorityDirect() throws TException {
    createTable("t1", "a;int", "b;int");
    String queryString =
        "CREATE TABLE xxx AS SELECT a as i, a + 1 as t, sum(b) as a, 2 * sum(b) as ta, 2 * sum(b + 3) as tat FROM t1 GROUP BY a";
    OutputCLL outputCLL =
        getOutputCLL(queryString, "default.xxx", "i;int", "t;int", "a;int", "ta;int", "tat;int");
    assertThat(outputCLL.getInputTables().keySet()).containsExactlyInAnyOrder("default.t1");
    assertCountColumnDependencies(outputCLL, 5);
    assertThat(outputCLL.getColumnDependencies().get("i").get("default.t1.a"))
        .containsExactlyInAnyOrder(TransformationInfo.identity());
    assertThat(outputCLL.getColumnDependencies().get("t").get("default.t1.a"))
        .containsExactlyInAnyOrder(TransformationInfo.transformation());
    assertThat(outputCLL.getColumnDependencies().get("a").get("default.t1.b"))
        .containsExactlyInAnyOrder(TransformationInfo.aggregation());
    assertThat(outputCLL.getColumnDependencies().get("ta").get("default.t1.b"))
        .containsExactlyInAnyOrder(TransformationInfo.aggregation());
    assertThat(outputCLL.getColumnDependencies().get("tat").get("default.t1.b"))
        .containsExactlyInAnyOrder(TransformationInfo.aggregation());
    assertCountDatasetDependencies(outputCLL, 1);
    assertThat(outputCLL.getDatasetDependencies().get("default.t1.a"))
        .containsExactlyInAnyOrder(TransformationInfo.indirect(GROUP_BY));
  }

  @Test
  void simpleQueryMasking() throws TException {
    createTable("t1", "a;int", "b;int");
    String queryString =
        "CREATE TABLE xxx AS SELECT "
            + "a as i, "
            + "a + 1 as t, "
            + "sha1(string(a + 1)) as mt, "
            + "sum(b) as a, "
            + "sha1(string(sum(b))) as ma "
            + "FROM t1 GROUP BY a";
    OutputCLL outputCLL =
        getOutputCLL(
            queryString, "default.xxx", "i;int", "t;int", "mt;string", "a;int", "ma;string");
    assertThat(outputCLL.getInputTables().keySet()).containsExactlyInAnyOrder("default.t1");
    assertCountColumnDependencies(outputCLL, 5);
    assertThat(outputCLL.getColumnDependencies().get("i").get("default.t1.a"))
        .containsExactlyInAnyOrder(TransformationInfo.identity());
    assertThat(outputCLL.getColumnDependencies().get("mt").get("default.t1.a"))
        .containsExactlyInAnyOrder(TransformationInfo.transformation(true));
    assertThat(outputCLL.getColumnDependencies().get("t").get("default.t1.a"))
        .containsExactlyInAnyOrder(TransformationInfo.transformation());
    assertThat(outputCLL.getColumnDependencies().get("a").get("default.t1.b"))
        .containsExactlyInAnyOrder(TransformationInfo.aggregation());
    assertThat(outputCLL.getColumnDependencies().get("ma").get("default.t1.b"))
        .containsExactlyInAnyOrder(TransformationInfo.aggregation(true));
    assertCountDatasetDependencies(outputCLL, 1);
    assertThat(outputCLL.getDatasetDependencies().get("default.t1.a"))
        .containsExactlyInAnyOrder(TransformationInfo.indirect(GROUP_BY));
  }

  @Test
  void simpleQueryWithCaseWhenConditional() throws TException {
    createTable("t1", "a;int", "b;int");
    String queryString =
        "CREATE TABLE xxx AS SELECT CASE WHEN b > 1 THEN a ELSE a + b END AS cond FROM t1";
    OutputCLL outputCLL = getOutputCLL(queryString, "default.xxx", "cond;int");
    assertThat(outputCLL.getInputTables().keySet()).containsExactlyInAnyOrder("default.t1");
    assertCountColumnDependencies(outputCLL, 4);
    assertThat(outputCLL.getColumnDependencies().get("cond").get("default.t1.a"))
        .isEqualTo(
            new HashSet<>(
                Arrays.asList(TransformationInfo.identity(), TransformationInfo.transformation())));
    assertThat(outputCLL.getColumnDependencies().get("cond").get("default.t1.b"))
        .isEqualTo(
            new HashSet<>(
                Arrays.asList(
                    TransformationInfo.transformation(),
                    TransformationInfo.indirect(CONDITIONAL))));
    assertCountDatasetDependencies(outputCLL, 0);
  }

  @Test
  void simpleQueryWithIfConditional() throws TException {
    createTable("t1", "a;int", "b;int");
    String queryString = "CREATE TABLE xxx AS SELECT IF(b > 1, a, a + b) AS cond FROM t1";
    OutputCLL outputCLL = getOutputCLL(queryString, "default.xxx", "cond;int");
    assertThat(outputCLL.getInputTables().keySet()).containsExactlyInAnyOrder("default.t1");
    assertCountColumnDependencies(outputCLL, 4);
    assertThat(outputCLL.getColumnDependencies().get("cond").get("default.t1.a"))
        .isEqualTo(
            new HashSet<>(
                Arrays.asList(TransformationInfo.identity(), TransformationInfo.transformation())));
    assertThat(outputCLL.getColumnDependencies().get("cond").get("default.t1.b"))
        .isEqualTo(
            new HashSet<>(
                Arrays.asList(
                    TransformationInfo.transformation(),
                    TransformationInfo.indirect(CONDITIONAL))));
    assertCountDatasetDependencies(outputCLL, 0);
  }

  @Test
  void simpleQueryExplode() throws TException {
    createTable("t1", "a;string");
    String queryString =
        "CREATE TABLE xxx AS SELECT a FROM (SELECT explode(split(a, ' ')) AS a FROM t1) subquery_alias";
    OutputCLL outputCLL = getOutputCLL(queryString, "default.xxx", "a;string");
    assertThat(outputCLL.getInputTables().keySet()).containsExactlyInAnyOrder("default.t1");
    assertCountColumnDependencies(outputCLL, 1);
    assertThat(outputCLL.getColumnDependencies().get("a").get("default.t1.a"))
        .containsExactlyInAnyOrder(TransformationInfo.transformation());
    assertCountDatasetDependencies(outputCLL, 0);
  }

  @Test
  void simpleQueryRank() throws TException {
    createTable("t1", "a;string", "b;string", "c;int");
    String queryString =
        "CREATE TABLE xxx AS\n"
            + "SELECT a, RANK() OVER (PARTITION BY b ORDER BY c) as rank FROM t1";
    OutputCLL outputCLL = getOutputCLL(queryString, "default.xxx", "a;string", "rank;int");
    assertThat(outputCLL.getInputTables().keySet()).containsExactlyInAnyOrder("default.t1");
    assertCountColumnDependencies(outputCLL, 3);
    assertThat(outputCLL.getColumnDependencies().get("a").get("default.t1.a"))
        .containsExactlyInAnyOrder(TransformationInfo.identity());
    assertThat(outputCLL.getColumnDependencies().get("rank").get("default.t1.b"))
        .containsExactlyInAnyOrder(TransformationInfo.indirect(WINDOW));
    assertThat(outputCLL.getColumnDependencies().get("rank").get("default.t1.c"))
        .containsExactlyInAnyOrder(TransformationInfo.indirect(WINDOW));
    assertCountDatasetDependencies(outputCLL, 0);
  }

  @Test
  void simpleQueryWindowedAggregate() throws TException {
    createTable("t1", "a;string", "b;string", "c;int");
    String queryString =
        "CREATE TABLE xxx AS\n" + "SELECT sum(a) OVER (PARTITION BY b ORDER BY c) AS s FROM t1";
    OutputCLL outputCLL = getOutputCLL(queryString, "default.xxx", "s;string");
    assertThat(outputCLL.getInputTables().keySet()).containsExactlyInAnyOrder("default.t1");
    assertCountColumnDependencies(outputCLL, 3);
    assertThat(outputCLL.getColumnDependencies().get("s").get("default.t1.a"))
        .containsExactlyInAnyOrder(TransformationInfo.aggregation());
    assertThat(outputCLL.getColumnDependencies().get("s").get("default.t1.b"))
        .containsExactlyInAnyOrder(TransformationInfo.indirect(WINDOW));
    assertThat(outputCLL.getColumnDependencies().get("s").get("default.t1.c"))
        .containsExactlyInAnyOrder(TransformationInfo.indirect(WINDOW));
    assertCountDatasetDependencies(outputCLL, 0);
  }

  @Test
  void simpleQueryWindowedTransformation() throws TException {
    createTable("t1", "a;int", "b;string", "c;int");
    String queryString =
        "CREATE TABLE xxx AS\n"
            + "SELECT LAG(a, 3, 0) OVER (PARTITION BY b ORDER BY c) AS l FROM t1";
    OutputCLL outputCLL = getOutputCLL(queryString, "default.xxx", "l;int");
    assertThat(outputCLL.getInputTables().keySet()).containsExactlyInAnyOrder("default.t1");
    assertCountColumnDependencies(outputCLL, 3);
    assertThat(outputCLL.getColumnDependencies().get("l").get("default.t1.a"))
        .containsExactlyInAnyOrder(TransformationInfo.transformation());
    assertThat(outputCLL.getColumnDependencies().get("l").get("default.t1.b"))
        .containsExactlyInAnyOrder(TransformationInfo.indirect(WINDOW));
    assertThat(outputCLL.getColumnDependencies().get("l").get("default.t1.c"))
        .containsExactlyInAnyOrder(TransformationInfo.indirect(WINDOW));
    assertCountDatasetDependencies(outputCLL, 0);
  }

  @Test
  void complexQueryCTEJoinsFilter() throws TException {
    createTable("t1", "a;int", "b;string");
    createTable("t2", "a;int", "c;int");
    createTable("t3", "a;int", "d;int");
    String queryString =
        "CREATE TABLE xxx AS \n"
            + "WITH tmp as (SELECT * FROM t1 where b = '1'),\n"
            + "tmp2 as (SELECT * FROM t2 where c = 1),\n"
            + "tmp3 as (SELECT tmp.a, b, c from tmp join tmp2 on tmp.a = tmp2.a)\n"
            + "SELECT tmp3.a as a, b, c, d FROM tmp3 join t3 on tmp3.a = t3.a order by d";
    OutputCLL outputCLL =
        getOutputCLL(queryString, "default.xxx", "a;int", "b;string", "c;int", "d;int");
    assertThat(outputCLL.getInputTables().keySet())
        .containsExactlyInAnyOrder("default.t1", "default.t2", "default.t3");
    assertCountColumnDependencies(outputCLL, 4);
    assertThat(outputCLL.getColumnDependencies().get("a").get("default.t1.a"))
        .containsExactlyInAnyOrder(TransformationInfo.identity());
    assertThat(outputCLL.getColumnDependencies().get("b").get("default.t1.b"))
        .containsExactlyInAnyOrder(TransformationInfo.identity());
    assertThat(outputCLL.getColumnDependencies().get("c").get("default.t2.c"))
        .containsExactlyInAnyOrder(TransformationInfo.identity());
    assertThat(outputCLL.getColumnDependencies().get("d").get("default.t3.d"))
        .containsExactlyInAnyOrder(TransformationInfo.identity());
    assertCountDatasetDependencies(outputCLL, 6);
    assertThat(outputCLL.getDatasetDependencies().get("default.t1.a"))
        .containsExactlyInAnyOrder(TransformationInfo.indirect(JOIN));
    assertThat(outputCLL.getDatasetDependencies().get("default.t2.a"))
        .containsExactlyInAnyOrder(TransformationInfo.indirect(JOIN));
    assertThat(outputCLL.getDatasetDependencies().get("default.t3.a"))
        .containsExactlyInAnyOrder(TransformationInfo.indirect(JOIN));
    assertThat(outputCLL.getDatasetDependencies().get("default.t1.b"))
        .containsExactlyInAnyOrder(TransformationInfo.indirect(FILTER));
    assertThat(outputCLL.getDatasetDependencies().get("default.t2.c"))
        .containsExactlyInAnyOrder(TransformationInfo.indirect(FILTER));
    assertThat(outputCLL.getDatasetDependencies().get("default.t3.d"))
        .containsExactlyInAnyOrder(TransformationInfo.indirect(SORT));
  }

  @Test
  public void union() throws TException {
    createTable("t1", "a;int", "b;string");
    createTable("t2", "a;int", "c;string");
    String queryString =
        "CREATE TABLE xxx AS\n"
            + "SELECT a, b, 'table1' as source\n"
            + "FROM t1\n"
            + "UNION ALL\n"
            + "SELECT a, c, 'table2' as source\n"
            + "FROM t2";
    OutputCLL outputCLL =
        getOutputCLL(queryString, "default.xxx", "a;int", "b;string", "source;string");
    assertThat(outputCLL.getInputTables().keySet())
        .containsExactlyInAnyOrder("default.t1", "default.t2");
    assertCountColumnDependencies(outputCLL, 4);
    assertThat(outputCLL.getColumnDependencies().get("a").get("default.t1.a"))
        .containsExactlyInAnyOrder(TransformationInfo.identity());
    assertThat(outputCLL.getColumnDependencies().get("a").get("default.t2.a"))
        .containsExactlyInAnyOrder(TransformationInfo.identity());
    assertThat(outputCLL.getColumnDependencies().get("b").get("default.t1.b"))
        .containsExactlyInAnyOrder(TransformationInfo.identity());
    assertThat(outputCLL.getColumnDependencies().get("b").get("default.t2.c"))
        .containsExactlyInAnyOrder(TransformationInfo.identity());
    assertCountDatasetDependencies(outputCLL, 0);
  }

  @Test
  void simpleInsertValues() throws TException {
    createTable("t1", "a;int", "b;string");
    String queryString = "INSERT INTO t1 VALUES(99, 'abcd')";
    OutputCLL outputCLL = getOutputCLL(queryString, "default.t1", "a;int", "b;string");
    assertThat(outputCLL.getInputTables().keySet()).isEmpty();
    assertCountColumnDependencies(outputCLL, 0);
    assertCountDatasetDependencies(outputCLL, 0);
  }

  @Test
  void simpleInsertFromTable() throws TException {
    createTable("t1", "a;int", "b;string");
    createTable("t2", "a;int", "b;string");
    String queryString = "INSERT INTO t1 SELECT a, concat(b, 'x') FROM t2";
    OutputCLL outputCLL = getOutputCLL(queryString, "default.t1", "a;int", "b;string");
    assertThat(outputCLL.getInputTables().keySet()).containsExactlyInAnyOrder("default.t2");
    assertCountColumnDependencies(outputCLL, 2);
    assertThat(outputCLL.getColumnDependencies().get("a").get("default.t2.a"))
        .containsExactlyInAnyOrder(TransformationInfo.identity());
    assertThat(outputCLL.getColumnDependencies().get("b").get("default.t2.b"))
        .containsExactlyInAnyOrder(TransformationInfo.transformation());
    assertCountDatasetDependencies(outputCLL, 0);
  }

  @Test
  void CTASWithJoins() throws TException {
    createTable("t1", "id;int", "name;string");
    createTable("t2", "name;string", "number;int");
    String queryString =
        String.join(
            "\n",
            "CREATE TABLE xxx AS",
            "WITH c AS (",
            "  SELECT b.name, a.id",
            "  FROM t1 a",
            "  JOIN t2 b",
            "    ON a.id = b.number",
            ")",
            "SELECT id * 10 as id, name FROM c");
    OutputCLL outputCLL = getOutputCLL(queryString, "default.xxx", "id;int", "name;string");
    assertThat(outputCLL.getInputTables().keySet())
        .containsExactlyInAnyOrder("default.t1", "default.t2");
    assertCountColumnDependencies(outputCLL, 2);
    assertThat(outputCLL.getColumnDependencies().get("id").get("default.t1.id"))
        .containsExactlyInAnyOrder(TransformationInfo.transformation());
    assertThat(outputCLL.getColumnDependencies().get("name").get("default.t2.name"))
        .containsExactlyInAnyOrder(TransformationInfo.identity());
    assertCountDatasetDependencies(outputCLL, 2);
    assertThat(outputCLL.getDatasetDependencies().get("default.t1.id"))
        .containsExactlyInAnyOrder(TransformationInfo.indirect(JOIN));
    assertThat(outputCLL.getDatasetDependencies().get("default.t2.number"))
        .containsExactlyInAnyOrder(TransformationInfo.indirect(JOIN));
  }

  @Test
  void multiInserts() throws TException {
    createTable("t1", "id;int", "name;string");
    createTable("t2", "id;int", "name;string");
    createTable("t3", "id;int", "name;string");
    createTable("t4", "id;int", "name;string");
    String queryString =
        String.join(
            "\n",
            "WITH c AS (",
            "  SELECT a.id, b.name ",
            "  FROM t3 a ",
            "  JOIN t4 b ",
            "    ON a.id = b.id",
            ")",
            "FROM c",
            "INSERT OVERWRITE TABLE t1",
            "  SELECT id * 10, name",
            "INSERT INTO TABLE t2",
            "  SELECT SUM(id), name",
            "  GROUP BY name");
    OutputCLL outputCLL1 = getOutputCLL(queryString, "default.t1", "id;int", "name;string");
    assertThat(outputCLL1.getInputTables().keySet())
        .containsExactlyInAnyOrder("default.t3", "default.t4");
    assertCountColumnDependencies(outputCLL1, 2);
    assertThat(outputCLL1.getColumnDependencies().get("id").get("default.t3.id"))
        .containsExactlyInAnyOrder(TransformationInfo.transformation());
    assertThat(outputCLL1.getColumnDependencies().get("name").get("default.t4.name"))
        .containsExactlyInAnyOrder(TransformationInfo.identity());
    assertCountDatasetDependencies(outputCLL1, 2);
    assertThat(outputCLL1.getDatasetDependencies().get("default.t3.id"))
        .containsExactlyInAnyOrder(TransformationInfo.indirect(JOIN));
    assertThat(outputCLL1.getDatasetDependencies().get("default.t4.id"))
        .containsExactlyInAnyOrder(TransformationInfo.indirect(JOIN));

    OutputCLL outputCLL2 = getOutputCLL(queryString, "default.t2", "id;int", "name;string");
    assertThat(outputCLL2.getInputTables().keySet())
        .containsExactlyInAnyOrder("default.t3", "default.t4");
    assertCountColumnDependencies(outputCLL2, 2);
    assertThat(outputCLL2.getColumnDependencies().get("id").get("default.t3.id"))
        .containsExactlyInAnyOrder(TransformationInfo.aggregation());
    assertThat(outputCLL2.getColumnDependencies().get("name").get("default.t4.name"))
        .containsExactlyInAnyOrder(TransformationInfo.identity());
    assertCountDatasetDependencies(outputCLL2, 3);
    assertThat(outputCLL2.getDatasetDependencies().get("default.t3.id"))
        .containsExactlyInAnyOrder(TransformationInfo.indirect(JOIN));
    assertThat(outputCLL2.getDatasetDependencies().get("default.t4.id"))
        .containsExactlyInAnyOrder(TransformationInfo.indirect(JOIN));
    assertThat(outputCLL2.getDatasetDependencies().get("default.t4.name"))
        .containsExactlyInAnyOrder(TransformationInfo.indirect(GROUP_BY));
  }

  @Test
  void caseInsensitivity() throws TException {
    createTable(
        "transactions",
        "submissiondate;date",
        "transactionamount;double",
        "transactiontype;string");
    String queryString =
        String.join(
            "\n",
            "CREATE TABLE monthly_transaction_summary",
            "AS",
            "SELECT",
            "    TRUNC(sUBMissionDAte, 'MM') AS MOnth,",
            "    transactionTYPe,",
            "    SUM(trANSactionamount) AS TotALAmount,",
            "    COUNT(*) AS transACTionCount",
            "FROM",
            "    tranSACTions",
            "GROUP BY",
            "    TRUNC(SUBMIssiondate, 'MM'),",
            "    tRANsacTIontype",
            "ORDER BY",
            "    monTH,",
            "    transacTIONtype");
    OutputCLL outputCLL =
        getOutputCLL(
            queryString,
            "default.monthly_transaction_summary",
            "month;date",
            "transactiontype;string",
            "totalamount;double",
            "transactioncount;bigint");
    assertThat(outputCLL.getInputTables().keySet())
        .containsExactlyInAnyOrder("default.transactions");
    assertCountColumnDependencies(outputCLL, 3);
    assertThat(
            outputCLL
                .getColumnDependencies()
                .get("month")
                .get("default.transactions.submissiondate"))
        .containsExactlyInAnyOrder(TransformationInfo.transformation());
    assertThat(
            outputCLL
                .getColumnDependencies()
                .get("transactiontype")
                .get("default.transactions.transactiontype"))
        .containsExactlyInAnyOrder(TransformationInfo.identity());
    assertThat(
            outputCLL
                .getColumnDependencies()
                .get("totalamount")
                .get("default.transactions.transactionamount"))
        .containsExactlyInAnyOrder(TransformationInfo.aggregation());
    assertCountDatasetDependencies(outputCLL, 4);
    assertThat(outputCLL.getDatasetDependencies().get("default.transactions.submissiondate"))
        .containsExactlyInAnyOrder(
            TransformationInfo.indirect(GROUP_BY), TransformationInfo.indirect(SORT));
    assertThat(outputCLL.getDatasetDependencies().get("default.transactions.transactiontype"))
        .containsExactlyInAnyOrder(
            TransformationInfo.indirect(GROUP_BY), TransformationInfo.indirect(SORT));
  }
}
