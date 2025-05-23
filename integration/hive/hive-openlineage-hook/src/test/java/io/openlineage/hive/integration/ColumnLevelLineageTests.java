/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.tuple;
import static org.mockserver.model.HttpRequest.request;

import io.openlineage.client.OpenLineage;
import io.openlineage.hive.testutils.MockServerTestUtils;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD")
@Tag("integration-test")
public class ColumnLevelLineageTests extends ContainerHiveTestBase {

  @Test
  void selectStar() {
    createManagedHiveTable("t1", "a int, b string");
    runHiveQuery("CREATE TABLE xxx AS SELECT * FROM t1");
    verifyEvents("cllSelectStar.json");
  }

  @Test
  void simpleQueryOnlyIdentity() {
    createManagedHiveTable("t1", "a int");
    runHiveQuery("CREATE TABLE xxx AS SELECT a FROM t1");
    verifyEvents("cllSimpleQueryOnlyIdentity.json");
  }

  @Test
  void ifNotExists() {
    createManagedHiveTable("t1", "a int");
    runHiveQuery("CREATE TABLE IF NOT EXISTS xxx AS SELECT a FROM t1");
    verifyEvents("cllIfNotExists.json");
  }

  @Test
  void partitionedTable() {
    createPartitionedHiveTable("t1", "a int", "b DATE");
    runHiveQuery("CREATE TABLE xxx AS SELECT * FROM t1");
    verifyEvents("cllPartitionedTable.json");
  }

  @Test
  void simpleQueryOnlyTransform() {
    createManagedHiveTable("t1", "a int, b int");
    runHiveQuery("CREATE TABLE xxx AS SELECT concat(a, 'test') AS a, a+b as b FROM t1");
    verifyEvents("cllSimpleQueryOnlyTransform.json");
  }

  @Test
  void dsimpleQueryOnlyAggregation() {
    createManagedHiveTable("t1", "a int");
    runHiveQuery("CREATE TABLE xxx AS SELECT count(a) AS a FROM t1");
    verifyEvents("cllSimpleQueryOnlyAggregation.json");
  }

  @Test
  void simpleQueryIndirect() {
    createManagedHiveTable("t1", "a int, b int");
    runHiveQuery("CREATE TABLE xxx AS SELECT a FROM t1 WHERE b > 1");
    verifyEvents("cllSimpleQueryIndirect.json");
  }

  @Test
  void simpleQueryMultipleIndirect() {
    createManagedHiveTable("t1", "a int, b int, c int");
    runHiveQuery("CREATE TABLE xxx AS SELECT a, c FROM t1 WHERE b > 1 GROUP BY a, c ORDER BY c");
    verifyEvents("cllSimpleQueryMultipleIndirect.json");
  }

  @Test
  void simpleQueryPriorityDirect() {
    createManagedHiveTable("t1", "a int, b int");
    runHiveQuery(
        "CREATE TABLE xxx AS SELECT a as i, a + 1 as t, sum(b) as a, 2 * sum(b) as ta, 2 * sum(b + 3) as tat FROM t1 GROUP BY a");
    verifyEvents("cllSimpleQueryPriorityDirect.json");
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
    verifyEvents("cllSimpleQueryMasking.json");
  }

  @Test
  void simpleQueryWithCaseWhenConditional() {
    createManagedHiveTable("t1", "a int, b int");
    runHiveQuery(
        "CREATE TABLE xxx AS SELECT CASE WHEN b > 1 THEN a ELSE a + b END AS cond FROM t1");
    verifyEvents("cllSimpleQueryWithCaseWhenConditional.json");
  }

  @Test
  void simpleQueryWithIfConditional() {
    createManagedHiveTable("t1", "a int, b int");
    runHiveQuery("CREATE TABLE xxx AS SELECT IF(b > 1, a, a + b) AS cond FROM t1");
    verifyEvents("cllSimpleQueryWithIfConditional.json");
  }

  @Test
  void simpleQueryExplode() {
    createManagedHiveTable("t1", "a string");
    runHiveQuery(
        "CREATE TABLE xxx AS SELECT a FROM (SELECT explode(split(a, ' ')) AS a FROM t1) subquery_alias");
    verifyEvents("cllSimpleQueryExplode.json");
  }

  @Test
  void simpleQueryRank() {
    createManagedHiveTable("t1", "a string, b string, c int");
    runHiveQuery(
        "CREATE TABLE xxx AS\n"
            + "SELECT a, RANK() OVER (PARTITION BY b ORDER BY c) as rank FROM t1");
    verifyEvents("cllSimpleQueryRank.json");
  }

  @Test
  void simpleQueryWindowedAggregate() {
    createManagedHiveTable("t1", "a string, b string, c int");
    runHiveQuery(
        "CREATE TABLE xxx AS\n" + "SELECT sum(a) OVER (PARTITION BY b ORDER BY c) AS s FROM t1");
    verifyEvents("cllSimpleQueryWindowedAggregate.json");
  }

  @Test
  void simpleQueryWindowedTransformation() {
    createManagedHiveTable("t1", "a int, b string, c int");
    runHiveQuery(
        "CREATE TABLE xxx AS\n"
            + "SELECT LAG(a, 3, 0) OVER (PARTITION BY b ORDER BY c) AS l FROM t1");
    verifyEvents("cllSimpleQueryWindowedTransformation.json");
  }

  @Test
  void complexQueryCTEJoinsFilter() {
    createManagedHiveTable("t1", "a int, b string");
    createManagedHiveTable("t2", "a int, c int");
    createManagedHiveTable("t3", "a int, d int");

    runHiveQuery(
        "CREATE TABLE xxx AS \n"
            + "WITH tmp as (SELECT * FROM t1 where b = '1'),\n"
            + "     tmp2 as (SELECT * FROM t2 where c = 1),\n"
            + "     tmp3 as (SELECT tmp.a, b, c from tmp join tmp2 on tmp.a = tmp2.a)\n"
            + "SELECT tmp3.a as a, b, c, d FROM tmp3 join t3 on tmp3.a = t3.a order by d");

    List<OpenLineage.RunEvent> events = getEventsEmitted();
    OpenLineage.RunEvent completedEvent =
        events.stream()
            .filter(e -> OpenLineage.RunEvent.EventType.COMPLETE.equals(e.getEventType()))
            .findFirst()
            .orElseThrow(() -> new AssertionError("No COMPLETE event found"));

    assertThat(completedEvent.getOutputs())
        .hasSize(1)
        .first()
        .satisfies(
            output -> {
              assertThat(output.getNamespace()).isEqualTo("hive://localhost:9083");
              assertThat(output.getName()).isEqualTo("test.xxx");

              OpenLineage.SchemaDatasetFacet schema = output.getFacets().getSchema();
              assertThat(schema).isNotNull();
              assertThat(schema.getFields())
                  .extracting("name", "type")
                  .containsExactly(
                      tuple("a", "int"),
                      tuple("b", "string"),
                      tuple("c", "int"),
                      tuple("d", "int"));

              OpenLineage.ColumnLineageDatasetFacet columnLineage =
                  output.getFacets().getColumnLineage();
              assertThat(columnLineage).isNotNull();

              assertThat(
                      columnLineage.getFields().getAdditionalProperties().get("a").getInputFields())
                  .hasSize(1)
                  .first()
                  .satisfies(
                      inputField -> {
                        assertThat(inputField.getNamespace()).isEqualTo("hive://localhost:9083");
                        assertThat(inputField.getName())
                            .isEqualTo("test.t1");
                        assertThat(inputField.getField()).isEqualTo("a");
                      });

              assertThat(
                      columnLineage.getFields().getAdditionalProperties().get("d").getInputFields())
                  .hasSize(1)
                  .first()
                  .satisfies(
                      inputField -> {
                        assertThat(inputField.getNamespace()).isEqualTo("hive://localhost:9083");
                        assertThat(inputField.getName())
                            .isEqualTo("test.t3");
                        assertThat(inputField.getField()).isEqualTo("d");
                      });

              OpenLineage.SymlinksDatasetFacet symlinks = output.getFacets().getSymlinks();
              assertThat(symlinks).isNotNull();
              assertThat(symlinks.getIdentifiers())
                  .hasSize(1)
                  .first()
                  .satisfies(
                      identifier -> {
                        assertThat(identifier.getNamespace()).isEqualTo("file");
                        assertThat(identifier.getName()).isEqualTo("/opt/hive/data/warehouse/test.db/xxx");
                        assertThat(identifier.getType()).isEqualTo("TABLE");
                      });
            });
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
    verifyEvents("cllUnionComplete.json");
  }

  @Test
  void simpleInsertValues() {
    createManagedHiveTable("t1", "a int, b string");
    runHiveQuery("INSERT INTO t1 VALUES(99, 'abcd')");
    assertThat(mockServerClient.retrieveRecordedRequests(request().withPath("/api/v1/lineage")))
        .isEmpty();
  }

  @Test
  void simpleInsertFromTable() {
    createManagedHiveTable("t1", "a int, b string");
    createManagedHiveTable("t2", "a int, b string");
    runHiveQuery("INSERT INTO t1 SELECT a, concat(b, 'x') FROM t2");
    verifyEvents("cllSimpleInsertFromTable.json");
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
    verifyEvents("cllCtasWithJoinsComplete.json");
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
    verifyEvents("cllMultiInsertsComplete.json");
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
    MockServerTestUtils.verifyEvents(mockServerClient, "cllCaseInsensitivityComplete.json");
  }
}
