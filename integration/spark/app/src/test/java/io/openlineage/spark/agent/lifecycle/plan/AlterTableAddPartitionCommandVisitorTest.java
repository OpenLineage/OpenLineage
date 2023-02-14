/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.SparkAgentTestExtension;
import java.util.List;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.execution.command.AlterTableAddPartitionCommand;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import scala.Option;
import scala.Tuple2;
import scala.collection.Seq;
import scala.collection.Seq$;
import scala.collection.immutable.Map;

@ExtendWith(SparkAgentTestExtension.class)
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
        scala.collection.immutable.Map$.MODULE$
            .<String, String>newBuilder()
            .$plus$eq(Tuple2.apply("col1", "aaa"))
            .result();

    Seq<Tuple2<Map<String, String>, Option<String>>> partitionSpecsAndLocs =
        Seq$.MODULE$
            .<Tuple2<Map<String, String>, Option<String>>>newBuilder()
            .$plus$eq(Tuple2.apply(params, Option.apply("file:///tmp/dir")))
            .result();

    AlterTableAddPartitionCommand command =
        new AlterTableAddPartitionCommand(
            new TableIdentifier(TABLE_5, Option.apply("default")), partitionSpecsAndLocs, false);

    command.run(session);

    assertThat(visitor.isDefinedAt(command)).isTrue();
    List<OpenLineage.OutputDataset> datasets = visitor.apply(command);
    assertEquals(2, datasets.get(0).getFacets().getSchema().getFields().size());
    assertEquals(
        "default.table5",
        datasets.get(0).getFacets().getSymlinks().getIdentifiers().get(0).getName());
    assertThat(datasets)
        .singleElement()
        .hasFieldOrPropertyWithValue("name", "/tmp/warehouse/table5")
        .hasFieldOrPropertyWithValue("namespace", "file");
  }
}
