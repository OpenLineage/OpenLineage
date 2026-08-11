/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.SparkAgentTestExtension;
import java.util.List;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier$;
import org.apache.spark.sql.execution.command.LoadDataCommand;
import org.junit.jupiter.api.Test;
import scala.Option;

class LoadDataCommandInputVisitorTest {

  @Test
  void testLoadDataCommandSourcePathIsAnInput() {
    SparkSession session =
        SparkSession.builder()
            .config("spark.sql.warehouse.dir", "/tmp/warehouse")
            .master("local")
            .getOrCreate();
    String database = session.catalog().currentDatabase();

    LoadDataCommandInputVisitor visitor =
        new LoadDataCommandInputVisitor(SparkAgentTestExtension.newContext(session));

    LoadDataCommand command =
        new LoadDataCommand(
            TableIdentifier$.MODULE$.apply("table", Option.apply(database)),
            "/path/to/data",
            true,
            false,
            Option.empty());

    assertThat(visitor.isDefinedAt(command)).isTrue();

    List<OpenLineage.InputDataset> datasets = visitor.apply(command);
    assertThat(datasets)
        .singleElement()
        .hasFieldOrPropertyWithValue("name", "/path/to/data")
        .hasFieldOrPropertyWithValue("namespace", "file");
  }
}
