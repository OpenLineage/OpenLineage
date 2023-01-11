/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.SparkAgentTestExtension;
import io.openlineage.spark.agent.lifecycle.CatalogTableTestUtils;
import java.util.List;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.execution.command.CreateTableCommand;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Option;

class CreateTableCommandVisitorTest {

  SparkSession session = mock(SparkSession.class);
  CreateTableCommandVisitor visitor;
  String database = "default";
  CreateTableCommand command;
  TableIdentifier table = new TableIdentifier("create_table", Option.apply(database));

  @BeforeEach
  public void setup() {
    when(session.sparkContext()).thenReturn(mock(SparkContext.class));
    command = new CreateTableCommand(CatalogTableTestUtils.getCatalogTable(table), true);
    visitor = new CreateTableCommandVisitor(SparkAgentTestExtension.newContext(session));
  }

  @Test
  void testCreateTableCommand() {
    List<OpenLineage.OutputDataset> datasets = visitor.apply(command);

    assertThat(visitor.isDefinedAt(command)).isTrue();
    OpenLineage.OutputDataset outputDataset = datasets.get(0);
    assertEquals(1, outputDataset.getFacets().getSchema().getFields().size());

    assertEquals(
        OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.CREATE,
        outputDataset.getFacets().getLifecycleStateChange().getLifecycleStateChange());
    assertEquals("/some-location", outputDataset.getName());
    assertEquals("file", outputDataset.getNamespace());
  }
}
