/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.SparkAgentTestExtension;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.util.List;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.execution.command.AlterTableSetLocationCommand;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import scala.Option;
import scala.collection.immutable.HashMap;

@ExtendWith(SparkAgentTestExtension.class)
class AlterTableSetLocationCommandVisitorTest {

  private static final String TABLE_1 = "table1";
  SparkSession session;
  AlterTableSetLocationCommandVisitor visitor;
  String database;

  @AfterEach
  public void afterEach() {
    dropTables();
  }

  private void dropTables() {
    session
        .sessionState()
        .catalog()
        .dropTable(new TableIdentifier(TABLE_1, Option.apply(database)), true, true);
  }

  @BeforeEach
  public void setup() {
    session =
        SparkSession.builder()
            .config("spark.sql.warehouse.dir", "/tmp/warehouse")
            .master("local")
            .getOrCreate();

    dropTables();

    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("col1", StringType$.MODULE$, false, new Metadata(new HashMap<>()))
            });

    session.catalog().createTable(TABLE_1, "csv", schema, ScalaConversionUtils.asScalaMapEmpty());
    database = session.catalog().currentDatabase();
    visitor = new AlterTableSetLocationCommandVisitor(SparkAgentTestExtension.newContext(session));
  }

  @Test
  void testAlterTableSetLocation() {
    AlterTableSetLocationCommand command =
        new AlterTableSetLocationCommand(
            new TableIdentifier(TABLE_1, Option.apply(database)),
            Option.empty(),
            "file:///tmp/dir");

    command.run(session);

    assertThat(visitor.isDefinedAt(command)).isTrue();
    List<OpenLineage.OutputDataset> datasets = visitor.apply(command);
    assertThat(datasets)
        .singleElement()
        .hasFieldOrPropertyWithValue("name", "/tmp/dir")
        .hasFieldOrPropertyWithValue("namespace", "file");

    assertThat(datasets.get(0).getFacets().getSchema().getFields()).hasSize(1);
    // custom location, no metastore -> no symlinks
    assertThat(datasets.get(0).getFacets().getSymlinks()).isNull();
  }
}
