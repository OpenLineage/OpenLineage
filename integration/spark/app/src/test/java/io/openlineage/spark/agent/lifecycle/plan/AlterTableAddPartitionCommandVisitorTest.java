/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.SparkAgentTestExtension;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.util.Collections;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession$;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.execution.command.AlterTableAddPartitionCommand;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import scala.Option;
import scala.Tuple2;
import scala.collection.immutable.Map;

@Tag("nonParallelTest")
class AlterTableAddPartitionCommandVisitorTest {

  private static final String TABLE_5 = "table5";
  SparkSession session;
  AlterTableAddPartitionCommandVisitor visitor;
  String database;

  @AfterEach
  public void afterEach() {
    dropTables();
  }

  private void dropTables() {
    session
        .sessionState()
        .catalog()
        .dropTable(new TableIdentifier(TABLE_5, Option.apply(database)), true, true);
  }

  @BeforeAll
  @SneakyThrows
  public static void beforeAll() {
    SparkSession$.MODULE$.cleanupAnyExistingSession();
  }

  @AfterAll
  @SneakyThrows
  public static void afterAll() {
    SparkSession$.MODULE$.cleanupAnyExistingSession();
  }

  @BeforeEach
  public void setup() {
    session =
        SparkSession.builder()
            .config("spark.sql.warehouse.dir", "/tmp/warehouse")
            .config("spark.sql.catalogImplementation", "hive")
            .enableHiveSupport()
            .master("local")
            .getOrCreate();

    dropTables();
    session.sql(
        "CREATE TABLE `table5` (col2 varchar(31)) PARTITIONED BY (\n"
            + "  `col1` string)\n"
            + "STORED AS PARQUET");
    visitor = new AlterTableAddPartitionCommandVisitor(SparkAgentTestExtension.newContext(session));
  }

  @Test
  void testAlterTableAddPartition() {

    scala.collection.immutable.Map<String, String> params =
        ScalaConversionUtils.<String, String>fromJavaMap(Collections.singletonMap("col1", "aaa"));

    scala.collection.immutable.Seq<Tuple2<Map<String, String>, Option<String>>>
        partitionSpecsAndLocs =
            ScalaConversionUtils.fromList(
                Collections.singletonList(Tuple2.apply(params, Option.apply("file:///tmp/dir"))));

    AlterTableAddPartitionCommand command =
        new AlterTableAddPartitionCommand(
            new TableIdentifier(TABLE_5, Option.apply("default")), partitionSpecsAndLocs, false);

    command.run(session);

    assertThat(visitor.isDefinedAt(command)).isTrue();
    List<OpenLineage.OutputDataset> datasets = visitor.apply(command);
    assertEquals(2, datasets.get(0).getFacets().getSchema().getFields().size());
    assertThat(datasets.get(0).getFacets().getSymlinks().getIdentifiers().get(0).getName())
        .endsWith("default.table5");
    assertThat(datasets.get(0).getName().endsWith("table5"));
    assertThat(datasets).singleElement().hasFieldOrPropertyWithValue("namespace", "file");
  }
}
