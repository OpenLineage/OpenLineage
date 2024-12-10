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
package io.openlineage.hive.integration;

import static io.openlineage.hive.hooks.TransformationInfo.Subtypes.CONDITIONAL;
import static io.openlineage.hive.hooks.TransformationInfo.Subtypes.FILTER;
import static io.openlineage.hive.hooks.TransformationInfo.Subtypes.GROUP_BY;
import static io.openlineage.hive.hooks.TransformationInfo.Subtypes.JOIN;
import static io.openlineage.hive.hooks.TransformationInfo.Subtypes.SORT;
import static io.openlineage.hive.hooks.TransformationInfo.Subtypes.WINDOW;
import static io.openlineage.hive.integration.ColumnLevelLineageTestUtils.assertColumnDependsOnType;
import static io.openlineage.hive.integration.ColumnLevelLineageTestUtils.assertCountColumnDependencies;
import static io.openlineage.hive.integration.ColumnLevelLineageTestUtils.assertCountDatasetDependencies;
import static io.openlineage.hive.integration.ColumnLevelLineageTestUtils.assertDatasetDependsOnType;
import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage;
import io.openlineage.hive.TestsBase;
import io.openlineage.hive.hooks.TransformationInfo;
import io.openlineage.hive.transport.DummyTransport;
import org.junit.jupiter.api.Test;

public class ColumnLevelLineageTests extends TestsBase {

  private static final String FILE = "file";

  @Test
  void selectStar() {
    createManagedHiveTable("t1", "a int, b string");
    runHiveQuery("CREATE TABLE xxx AS SELECT * FROM t1");
    OpenLineage.ColumnLineageDatasetFacet facet = DummyTransport.getColumnLineage("xxx");
    ColumnLevelLineageTestUtils.assertCountColumnDependencies(facet, 2);
    assertColumnDependsOnType(facet, "a", FILE, "t1", "a", TransformationInfo.identity());
    assertColumnDependsOnType(facet, "b", FILE, "t1", "b", TransformationInfo.identity());
    ColumnLevelLineageTestUtils.assertCountDatasetDependencies(facet, 0);
  }

  @Test
  void simpleQueryOnlyIdentity() {
    createManagedHiveTable("t1", "a int");
    runHiveQuery("CREATE TABLE xxx AS SELECT a FROM t1");
    OpenLineage.ColumnLineageDatasetFacet facet = DummyTransport.getColumnLineage("xxx");
    ColumnLevelLineageTestUtils.assertCountColumnDependencies(facet, 1);
    assertColumnDependsOnType(facet, "a", FILE, "t1", "a", TransformationInfo.identity());
    ColumnLevelLineageTestUtils.assertCountDatasetDependencies(facet, 0);
  }

  @Test
  void ifNotExists() {
    createManagedHiveTable("t1", "a int");
    runHiveQuery("CREATE TABLE IF NOT EXISTS xxx AS SELECT a FROM t1");
    OpenLineage.ColumnLineageDatasetFacet facet = DummyTransport.getColumnLineage("xxx");
    ColumnLevelLineageTestUtils.assertCountColumnDependencies(facet, 1);
    assertColumnDependsOnType(facet, "a", FILE, "t1", "a", TransformationInfo.identity());
    ColumnLevelLineageTestUtils.assertCountDatasetDependencies(facet, 0);
  }

  @Test
  void partitionedTable() {
    createPartitionedHiveTable("t1", "a int", "b DATE");
    runHiveQuery("CREATE TABLE xxx AS SELECT * FROM t1");
    OpenLineage.ColumnLineageDatasetFacet facet = DummyTransport.getColumnLineage("xxx");
    ColumnLevelLineageTestUtils.assertCountColumnDependencies(facet, 2);
    assertColumnDependsOnType(facet, "a", FILE, "t1", "a", TransformationInfo.identity());
    assertColumnDependsOnType(facet, "b", FILE, "t1", "b", TransformationInfo.identity());
    ColumnLevelLineageTestUtils.assertCountDatasetDependencies(facet, 0);
  }

  @Test
  void simpleQueryOnlyTransform() {
    createManagedHiveTable("t1", "a int, b int");
    runHiveQuery("CREATE TABLE xxx AS SELECT concat(a, 'test') AS a, a+b as b FROM t1");
    OpenLineage.ColumnLineageDatasetFacet facet = DummyTransport.getColumnLineage("xxx");
    ColumnLevelLineageTestUtils.assertCountColumnDependencies(facet, 3);
    assertColumnDependsOnType(facet, "a", FILE, "t1", "a", TransformationInfo.transformation());
    assertColumnDependsOnType(facet, "b", FILE, "t1", "a", TransformationInfo.transformation());
    assertColumnDependsOnType(facet, "b", FILE, "t1", "b", TransformationInfo.transformation());
    ColumnLevelLineageTestUtils.assertCountDatasetDependencies(facet, 0);
  }

  @Test
  void simpleQueryOnlyAggregation() {
    createManagedHiveTable("t1", "a int");
    runHiveQuery("CREATE TABLE xxx AS SELECT count(a) AS a FROM t1");
    OpenLineage.ColumnLineageDatasetFacet facet = DummyTransport.getColumnLineage("xxx");
    assertCountColumnDependencies(facet, 1);
    assertColumnDependsOnType(facet, "a", FILE, "t1", "a", TransformationInfo.aggregation(true));
    assertCountDatasetDependencies(facet, 0);
  }

  @Test
  void simpleQueryIndirect() {
    createManagedHiveTable("t1", "a int, b int");
    runHiveQuery("CREATE TABLE xxx AS SELECT a FROM t1 WHERE b > 1");
    OpenLineage.ColumnLineageDatasetFacet facet = DummyTransport.getColumnLineage("xxx");
    assertCountColumnDependencies(facet, 1);
    assertColumnDependsOnType(facet, "a", FILE, "t1", "a", TransformationInfo.identity());
    assertCountDatasetDependencies(facet, 1);
    assertDatasetDependsOnType(facet, FILE, "t1", "b", TransformationInfo.indirect(FILTER));
  }

  @Test
  void simpleQueryMultipleIndirect() {
    createManagedHiveTable("t1", "a int, b int, c int");
    runHiveQuery("CREATE TABLE xxx AS SELECT a, c FROM t1 WHERE b > 1 GROUP BY a, c ORDER BY c");
    OpenLineage.ColumnLineageDatasetFacet facet = DummyTransport.getColumnLineage("xxx");
    assertCountColumnDependencies(facet, 2);
    assertColumnDependsOnType(facet, "a", FILE, "t1", "a", TransformationInfo.identity());
    assertColumnDependsOnType(facet, "c", FILE, "t1", "c", TransformationInfo.identity());
    assertCountDatasetDependencies(facet, 4);
    assertDatasetDependsOnType(facet, FILE, "t1", "a", TransformationInfo.indirect(GROUP_BY));
    assertDatasetDependsOnType(facet, FILE, "t1", "b", TransformationInfo.indirect(FILTER));
    assertDatasetDependsOnType(facet, FILE, "t1", "c", TransformationInfo.indirect(GROUP_BY));
    assertDatasetDependsOnType(facet, FILE, "t1", "c", TransformationInfo.indirect(SORT));
  }

  @Test
  void simpleQueryPriorityDirect() {
    createManagedHiveTable("t1", "a int, b int");
    runHiveQuery(
        "CREATE TABLE xxx AS SELECT a as i, a + 1 as t, sum(b) as a, 2 * sum(b) as ta, 2 * sum(b + 3) as tat FROM t1 GROUP BY a");
    OpenLineage.ColumnLineageDatasetFacet facet = DummyTransport.getColumnLineage("xxx");
    assertCountColumnDependencies(facet, 5);
    assertColumnDependsOnType(facet, "i", FILE, "t1", "a", TransformationInfo.identity());
    assertColumnDependsOnType(facet, "t", FILE, "t1", "a", TransformationInfo.transformation());
    assertColumnDependsOnType(facet, "a", FILE, "t1", "b", TransformationInfo.aggregation());
    assertColumnDependsOnType(facet, "ta", FILE, "t1", "b", TransformationInfo.aggregation());
    assertColumnDependsOnType(facet, "tat", FILE, "t1", "b", TransformationInfo.aggregation());
    assertCountDatasetDependencies(facet, 1);
    assertDatasetDependsOnType(facet, FILE, "t1", "a", TransformationInfo.indirect(GROUP_BY));
  }

  @Test
  void simpleQueryMasking() {
    createManagedHiveTable("t1", "a int, b int");
    runHiveQuery(
        "CREATE TABLE xxx AS SELECT "
            + "a as i, "
            + "a + 1 as t, "
            + "sha1(string(a + 1)) as mt, "
            + "sum(b) as a, "
            + "sha1(string(sum(b))) as ma "
            + "FROM t1 GROUP BY a");
    OpenLineage.ColumnLineageDatasetFacet facet = DummyTransport.getColumnLineage("xxx");
    assertCountColumnDependencies(facet, 5);
    assertColumnDependsOnType(facet, "i", FILE, "t1", "a", TransformationInfo.identity());
    assertColumnDependsOnType(
        facet, "mt", FILE, "t1", "a", TransformationInfo.transformation(true));
    assertColumnDependsOnType(facet, "t", FILE, "t1", "a", TransformationInfo.transformation());
    assertColumnDependsOnType(facet, "a", FILE, "t1", "b", TransformationInfo.aggregation());
    assertColumnDependsOnType(facet, "ma", FILE, "t1", "b", TransformationInfo.aggregation(true));
    assertCountDatasetDependencies(facet, 1);
    assertDatasetDependsOnType(facet, FILE, "t1", "a", TransformationInfo.indirect(GROUP_BY));
  }

  @Test
  void simpleQueryWithCaseWhenConditional() {
    createManagedHiveTable("t1", "a int, b int");
    runHiveQuery(
        "CREATE TABLE xxx AS SELECT CASE WHEN b > 1 THEN a ELSE a + b END AS cond FROM t1");
    OpenLineage.ColumnLineageDatasetFacet facet = DummyTransport.getColumnLineage("xxx");
    assertCountColumnDependencies(facet, 4);
    assertColumnDependsOnType(facet, "cond", FILE, "t1", "a", TransformationInfo.identity());
    assertColumnDependsOnType(facet, "cond", FILE, "t1", "a", TransformationInfo.transformation());
    assertColumnDependsOnType(facet, "cond", FILE, "t1", "b", TransformationInfo.transformation());
    assertColumnDependsOnType(
        facet, "cond", FILE, "t1", "b", TransformationInfo.indirect(CONDITIONAL));
    assertCountDatasetDependencies(facet, 0);
  }

  @Test
  void simpleQueryWithIfConditional() {
    createManagedHiveTable("t1", "a int, b int");
    runHiveQuery("CREATE TABLE xxx AS SELECT IF(b > 1, a, a + b) AS cond FROM t1");
    OpenLineage.ColumnLineageDatasetFacet facet = DummyTransport.getColumnLineage("xxx");
    assertCountColumnDependencies(facet, 4);
    assertColumnDependsOnType(facet, "cond", FILE, "t1", "a", TransformationInfo.identity());
    assertColumnDependsOnType(facet, "cond", FILE, "t1", "a", TransformationInfo.transformation());
    assertColumnDependsOnType(facet, "cond", FILE, "t1", "b", TransformationInfo.transformation());
    assertColumnDependsOnType(
        facet, "cond", FILE, "t1", "b", TransformationInfo.indirect(CONDITIONAL));
    assertCountDatasetDependencies(facet, 0);
  }

  @Test
  void simpleQueryExplode() {
    createManagedHiveTable("t1", "a string");
    runHiveQuery(
        "CREATE TABLE xxx AS SELECT a FROM (SELECT explode(split(a, ' ')) AS a FROM t1) subquery_alias");
    OpenLineage.ColumnLineageDatasetFacet facet = DummyTransport.getColumnLineage("xxx");
    assertCountColumnDependencies(facet, 1);
    assertColumnDependsOnType(facet, "a", FILE, "t1", "a", TransformationInfo.transformation());
    assertCountDatasetDependencies(facet, 0);
  }

  @Test
  void simpleQueryRank() {
    createManagedHiveTable("t1", "a string, b string, c int");
    runHiveQuery(
        "CREATE TABLE xxx AS\n"
            + "SELECT a, RANK() OVER (PARTITION BY b ORDER BY c) as rank FROM t1");
    OpenLineage.ColumnLineageDatasetFacet facet = DummyTransport.getColumnLineage("xxx");
    assertCountColumnDependencies(facet, 3);
    assertColumnDependsOnType(facet, "a", FILE, "t1", "a", TransformationInfo.identity());
    assertColumnDependsOnType(facet, "rank", FILE, "t1", "b", TransformationInfo.indirect(WINDOW));
    assertColumnDependsOnType(facet, "rank", FILE, "t1", "c", TransformationInfo.indirect(WINDOW));
    assertCountDatasetDependencies(facet, 0);
  }

  @Test
  void simpleQueryWindowedAggregate() {
    createManagedHiveTable("t1", "a string, b string, c int");
    runHiveQuery(
        "CREATE TABLE xxx AS\n" + "SELECT sum(a) OVER (PARTITION BY b ORDER BY c) AS s FROM t1");
    OpenLineage.ColumnLineageDatasetFacet facet = DummyTransport.getColumnLineage("xxx");
    assertCountColumnDependencies(facet, 3);
    assertColumnDependsOnType(facet, "s", FILE, "t1", "a", TransformationInfo.aggregation());
    assertColumnDependsOnType(facet, "s", FILE, "t1", "b", TransformationInfo.indirect(WINDOW));
    assertColumnDependsOnType(facet, "s", FILE, "t1", "c", TransformationInfo.indirect(WINDOW));
    assertCountDatasetDependencies(facet, 0);
  }

  @Test
  void simpleQueryWindowedTransformation() {
    createManagedHiveTable("t1", "a int, b string, c int");
    runHiveQuery(
        "CREATE TABLE xxx AS\n"
            + "SELECT LAG(a, 3, 0) OVER (PARTITION BY b ORDER BY c) AS l FROM t1");
    OpenLineage.ColumnLineageDatasetFacet facet = DummyTransport.getColumnLineage("xxx");
    assertCountColumnDependencies(facet, 3);
    assertColumnDependsOnType(facet, "l", FILE, "t1", "a", TransformationInfo.transformation());
    assertColumnDependsOnType(facet, "l", FILE, "t1", "b", TransformationInfo.indirect(WINDOW));
    assertColumnDependsOnType(facet, "l", FILE, "t1", "c", TransformationInfo.indirect(WINDOW));
    assertCountDatasetDependencies(facet, 0);
  }

  @Test
  void complexQueryCTEJoinsFilter() {
    createManagedHiveTable("t1", "a int, b string");
    createManagedHiveTable("t2", "a int, c int");
    createManagedHiveTable("t3", "a int, d int");
    runHiveQuery(
        "CREATE TABLE xxx AS \n"
            + "WITH tmp as (SELECT * FROM t1 where b = '1'),\n"
            + "tmp2 as (SELECT * FROM t2 where c = 1),\n"
            + "tmp3 as (SELECT tmp.a, b, c from tmp join tmp2 on tmp.a = tmp2.a)\n"
            + "SELECT tmp3.a as a, b, c, d FROM tmp3 join t3 on tmp3.a = t3.a order by d");
    OpenLineage.ColumnLineageDatasetFacet facet = DummyTransport.getColumnLineage("xxx");
    assertCountColumnDependencies(facet, 4);
    assertColumnDependsOnType(facet, "a", FILE, "t1", "a", TransformationInfo.identity());
    assertColumnDependsOnType(facet, "b", FILE, "t1", "b", TransformationInfo.identity());
    assertColumnDependsOnType(facet, "c", FILE, "t2", "c", TransformationInfo.identity());
    assertColumnDependsOnType(facet, "d", FILE, "t3", "d", TransformationInfo.identity());
    assertCountDatasetDependencies(facet, 6);
    assertDatasetDependsOnType(facet, FILE, "t1", "a", TransformationInfo.indirect(JOIN));
    assertDatasetDependsOnType(facet, FILE, "t2", "a", TransformationInfo.indirect(JOIN));
    assertDatasetDependsOnType(facet, FILE, "t3", "a", TransformationInfo.indirect(JOIN));
    assertDatasetDependsOnType(facet, FILE, "t1", "b", TransformationInfo.indirect(FILTER));
    assertDatasetDependsOnType(facet, FILE, "t2", "c", TransformationInfo.indirect(FILTER));
    assertDatasetDependsOnType(facet, FILE, "t3", "d", TransformationInfo.indirect(SORT));
  }

  @Test
  public void union() {
    createManagedHiveTable("t1", "a int, b string");
    createManagedHiveTable("t2", "a int, c string");
    runHiveQuery(
        "CREATE TABLE xxx AS\n"
            + "SELECT a, b, 'table1' as source\n"
            + "FROM t1\n"
            + "UNION ALL\n"
            + "SELECT a, c, 'table2' as source\n"
            + "FROM t2");
    OpenLineage.ColumnLineageDatasetFacet facet = DummyTransport.getColumnLineage("xxx");
    assertCountColumnDependencies(facet, 4);
    assertColumnDependsOnType(facet, "a", FILE, "t1", "a", TransformationInfo.identity());
    assertColumnDependsOnType(facet, "a", FILE, "t2", "a", TransformationInfo.identity());
    assertColumnDependsOnType(facet, "b", FILE, "t1", "b", TransformationInfo.identity());
    assertColumnDependsOnType(facet, "b", FILE, "t2", "c", TransformationInfo.identity());
    assertCountDatasetDependencies(facet, 0);
  }

  @Test
  void simpleInsertValues() {
    createManagedHiveTable("t1", "a int, b string");
    runHiveQuery("INSERT INTO t1 VALUES(99, 'abcd')");
    assertThat(DummyTransport.getEvents()).hasSize(0);
  }

  @Test
  void simpleInsertFromTable() {
    createManagedHiveTable("t1", "a int, b string");
    createManagedHiveTable("t2", "a int, b string");
    runHiveQuery("INSERT INTO t1 SELECT a, concat(b, 'x') FROM t2");
    OpenLineage.ColumnLineageDatasetFacet facet = DummyTransport.getColumnLineage("t1");
    assertCountColumnDependencies(facet, 2);
    assertColumnDependsOnType(facet, "a", FILE, "t2", "a", TransformationInfo.identity());
    assertColumnDependsOnType(facet, "b", FILE, "t2", "b", TransformationInfo.transformation());
    assertCountDatasetDependencies(facet, 0);
  }

  @Test
  void CTASWithJoins() {
    createManagedHiveTable("t1", "id int, name string");
    createManagedHiveTable("t2", "name string, number int");
    runHiveQuery(
        String.join(
            "\n",
            "CREATE TABLE xxx AS",
            "WITH c AS (",
            "  SELECT b.name, a.id",
            "  FROM t1 a",
            "  JOIN t2 b",
            "    ON a.id = b.number",
            ")",
            "SELECT id * 10 as id, name FROM c"));
    OpenLineage.ColumnLineageDatasetFacet facet = DummyTransport.getColumnLineage("xxx");
    assertCountColumnDependencies(facet, 2);
    assertColumnDependsOnType(facet, "id", FILE, "t1", "id", TransformationInfo.transformation());
    assertColumnDependsOnType(facet, "name", FILE, "t2", "name", TransformationInfo.identity());
    assertCountDatasetDependencies(facet, 2);
    assertDatasetDependsOnType(facet, FILE, "t1", "id", TransformationInfo.indirect(JOIN));
    assertDatasetDependsOnType(facet, FILE, "t2", "number", TransformationInfo.indirect(JOIN));
  }

  @Test
  void multiInserts() {
    createManagedHiveTable("t1", "id int, name string");
    createManagedHiveTable("t2", "id int, name string");
    createManagedHiveTable("t3", "id int, name string");
    createManagedHiveTable("t4", "id int, name string");
    runHiveQuery(
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
            "  GROUP BY name;"));
    OpenLineage.ColumnLineageDatasetFacet t1Facet = DummyTransport.getColumnLineage("t1");
    assertCountColumnDependencies(t1Facet, 2);
    assertColumnDependsOnType(t1Facet, "id", FILE, "t3", "id", TransformationInfo.transformation());
    assertColumnDependsOnType(t1Facet, "name", FILE, "t4", "name", TransformationInfo.identity());
    assertCountDatasetDependencies(t1Facet, 2);
    assertDatasetDependsOnType(t1Facet, FILE, "t3", "id", TransformationInfo.indirect(JOIN));
    assertDatasetDependsOnType(t1Facet, FILE, "t4", "id", TransformationInfo.indirect(JOIN));

    OpenLineage.ColumnLineageDatasetFacet t2Facet = DummyTransport.getColumnLineage("t2");
    assertCountColumnDependencies(t2Facet, 2);
    assertColumnDependsOnType(t2Facet, "id", FILE, "t3", "id", TransformationInfo.aggregation());
    assertColumnDependsOnType(t2Facet, "name", FILE, "t4", "name", TransformationInfo.identity());
    assertCountDatasetDependencies(t2Facet, 3);
    assertDatasetDependsOnType(t2Facet, FILE, "t3", "id", TransformationInfo.indirect(JOIN));
    assertDatasetDependsOnType(t2Facet, FILE, "t4", "id", TransformationInfo.indirect(JOIN));
    assertDatasetDependsOnType(t2Facet, FILE, "t4", "name", TransformationInfo.indirect(GROUP_BY));
  }

  @Test
  void caseInsensitivity() {
    createManagedHiveTable(
        "traNSactions", "SUBmissionDate DATE, tranSActionAmount DOUBLE, TRansacTIonType STRING");
    runHiveQuery(
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
            "    transacTIONtype"));
    OpenLineage.ColumnLineageDatasetFacet facet =
        DummyTransport.getColumnLineage("monthly_transaction_summary");
    assertCountColumnDependencies(facet, 3);
    assertColumnDependsOnType(
        facet,
        "month",
        FILE,
        "transactions",
        "submissiondate",
        TransformationInfo.transformation());
    assertColumnDependsOnType(
        facet,
        "transactiontype",
        FILE,
        "transactions",
        "transactiontype",
        TransformationInfo.identity());
    assertColumnDependsOnType(
        facet,
        "totalamount",
        FILE,
        "transactions",
        "transactionamount",
        TransformationInfo.aggregation());
    assertCountDatasetDependencies(facet, 4);
    assertDatasetDependsOnType(
        facet, FILE, "transactions", "submissiondate", TransformationInfo.indirect(GROUP_BY));
    assertDatasetDependsOnType(
        facet, FILE, "transactions", "transactiontype", TransformationInfo.indirect(GROUP_BY));
    assertDatasetDependsOnType(
        facet, FILE, "transactions", "submissiondate", TransformationInfo.indirect(SORT));
    assertDatasetDependsOnType(
        facet, FILE, "transactions", "transactiontype", TransformationInfo.indirect(SORT));
  }
}
