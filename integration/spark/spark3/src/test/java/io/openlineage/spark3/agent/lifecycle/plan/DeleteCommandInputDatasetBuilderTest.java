/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Collections;
import java.util.List;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.AppendData;
import org.apache.spark.sql.catalyst.plans.logical.DeleteFromTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.OneRowRelation;
import org.apache.spark.sql.catalyst.plans.logical.UpdateTable;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.junit.jupiter.api.Test;
import scala.PartialFunction;
import scala.runtime.AbstractPartialFunction;

class DeleteCommandInputDatasetBuilderTest {

  private static final OpenLineage OPEN_LINEAGE =
      new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);

  OpenLineageContext context = mock(OpenLineageContext.class);
  DeleteCommandInputDatasetBuilder builder = new DeleteCommandInputDatasetBuilder(context);
  SparkListenerEvent event = new SparkListenerSQLExecutionEnd(1L, 1L);

  @Test
  void testIsDefinedAtLogicalPlan() {
    assertTrue(builder.isDefinedAtLogicalPlan(mock(DeleteFromTable.class)));
    assertFalse(builder.isDefinedAtLogicalPlan(mock(UpdateTable.class)));
    assertFalse(builder.isDefinedAtLogicalPlan(mock(AppendData.class)));
    assertFalse(builder.isDefinedAtLogicalPlan(mock(LogicalPlan.class)));
  }

  @Test
  void testApplyCollectsInputsFromSubqueries() {
    givenSubqueryVisitorReturning("subquery_table", "subquery_namespace");

    DeleteFromTable plan = mock(DeleteFromTable.class);
    when(plan.subqueries())
        .thenReturn(
            ScalaConversionUtils.fromList(
                Collections.<LogicalPlan>singletonList(new OneRowRelation())));

    List<InputDataset> inputs = builder.apply(event, plan);

    assertThat(inputs)
        .singleElement()
        .hasFieldOrPropertyWithValue("name", "subquery_table")
        .hasFieldOrPropertyWithValue("namespace", "subquery_namespace");
  }

  @Test
  void testApplyWithoutSubqueries() {
    givenSubqueryVisitorReturning("subquery_table", "subquery_namespace");

    DeleteFromTable plan = mock(DeleteFromTable.class);
    when(plan.subqueries()).thenReturn(ScalaConversionUtils.asScalaSeqEmpty());

    assertThat(builder.apply(event, plan)).isEmpty();
  }

  /**
   * Registers a visitor that turns the subquery plan node into a known dataset, standing in for the
   * relation builders that resolve a real table.
   */
  private void givenSubqueryVisitorReturning(String name, String namespace) {
    PartialFunction<LogicalPlan, List<InputDataset>> visitor =
        new AbstractPartialFunction<LogicalPlan, List<InputDataset>>() {
          @Override
          public boolean isDefinedAt(LogicalPlan x) {
            return x instanceof OneRowRelation;
          }

          @Override
          public List<InputDataset> apply(LogicalPlan x) {
            return Collections.singletonList(
                OPEN_LINEAGE.newInputDatasetBuilder().name(name).namespace(namespace).build());
          }
        };

    when(context.getInputDatasetQueryPlanVisitors()).thenReturn(Collections.singletonList(visitor));
    when(context.getInputDatasetBuilders()).thenReturn(Collections.emptyList());
  }
}
