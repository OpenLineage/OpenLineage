/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.SparkAgentTestExtension;
import java.util.List;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.execution.command.AlterTableRenameCommand;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import scala.Option;
import scala.collection.Map$;
import scala.collection.immutable.HashMap;

@ExtendWith(SparkAgentTestExtension.class)
class AlterTableRenameCommandVisitorTest {

  private static final String OLD_TABLE = "old_table";
  private static final String NEW_TABLE = "new_table";
  SparkSession session;
  AlterTableRenameCommandVisitor visitor;
  String database;

  @AfterEach
  public void afterEach() {
    dropTables();
  }

  private void dropTables() {
    session
        .sessionState()
        .catalog()
        .dropTable(new TableIdentifier(OLD_TABLE, Option.apply(database)), true, true);
    session
        .sessionState()
        .catalog()
        .dropTable(new TableIdentifier(NEW_TABLE, Option.apply(database)), true, true);
  }

  @BeforeEach
  public void setup() {
    session =
        SparkSession.builder()
            .config("spark.sql.warehouse.dir", "/tmp/warehouse")
            .master("local")
            .getOrCreate();

    database = session.catalog().currentDatabase();
    dropTables();

    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("a", StringType$.MODULE$, false, new Metadata(new HashMap<>()))
            });

    session.catalog().createTable(OLD_TABLE, "csv", schema, Map$.MODULE$.empty());
    visitor = new AlterTableRenameCommandVisitor(SparkAgentTestExtension.newContext(session));
  }

  @Test
  void testAlterRenameCommandCommand() {
    AlterTableRenameCommand command =
        new AlterTableRenameCommand(
            new TableIdentifier(OLD_TABLE, Option.apply(database)),
            new TableIdentifier(NEW_TABLE, Option.apply(database)),
            false);
    command.run(session);

    assertThat(visitor.isDefinedAt(command)).isTrue();
    List<OpenLineage.OutputDataset> datasets = visitor.apply(command);
    assertThat(datasets)
        .singleElement()
        .hasFieldOrPropertyWithValue("name", "/tmp/warehouse/new_table")
        .hasFieldOrPropertyWithValue("namespace", "file");

    OpenLineage.LifecycleStateChangeDatasetFacetPreviousIdentifier previousIdentifier =
        datasets.get(0).getFacets().getLifecycleStateChange().getPreviousIdentifier();

    assertThat(previousIdentifier.getName()).isEqualTo("/tmp/warehouse/old_table");
    assertThat(previousIdentifier.getNamespace()).isEqualTo("file");
  }

  @Test
  void testAlterRenameCommandCommandVisitorBeforeCommandRun() {
    AlterTableRenameCommand command =
        new AlterTableRenameCommand(
            new TableIdentifier(OLD_TABLE, Option.apply(database)),
            new TableIdentifier(NEW_TABLE, Option.apply(database)),
            false);

    // command is not run
    assertThat(visitor.isDefinedAt(command)).isTrue();
    List<OpenLineage.OutputDataset> datasets = visitor.apply(command);
    assertThat(datasets).isEmpty();
  }
}
