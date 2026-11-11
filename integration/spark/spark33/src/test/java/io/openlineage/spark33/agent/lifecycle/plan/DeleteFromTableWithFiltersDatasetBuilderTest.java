/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark33.agent.lifecycle.plan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.DeleteFromTableWithFilters;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.junit.jupiter.api.Test;

class DeleteFromTableWithFiltersDatasetBuilderTest {

  OpenLineageContext openLineageContext =
      OpenLineageContext.builder()
          .sparkSession(mock(SparkSession.class))
          .sparkContext(mock(SparkContext.class))
          .openLineage(mock(OpenLineage.class))
          .meterRegistry(new SimpleMeterRegistry())
          .openLineageConfig(new SparkOpenLineageConfig())
          .build();

  DeleteFromTableWithFiltersDatasetBuilder builder =
      new DeleteFromTableWithFiltersDatasetBuilder(openLineageContext);

  DeleteFromTableWithFilters plan = mock(DeleteFromTableWithFilters.class);
  DataSourceV2Relation table = mock(DataSourceV2Relation.class);

  @Test
  void testIsDefinedAtLogicalPlan() {
    when(plan.table()).thenReturn(table);
    assertThat(builder.isDefinedAtLogicalPlan(plan)).isTrue();
    assertThat(builder.isDefinedAtLogicalPlan(mock(LogicalPlan.class))).isFalse();

    DeleteFromTableWithFilters nonV2Plan = mock(DeleteFromTableWithFilters.class);
    when(nonV2Plan.table()).thenReturn(mock(LogicalPlan.class));
    assertThat(builder.isDefinedAtLogicalPlan(nonV2Plan)).isFalse();
  }

  @Test
  void testApplyDoesNotExtractOutputs() {
    when(plan.table()).thenReturn(table);
    assertThat(builder.apply(mock(SparkListenerSQLExecutionEnd.class), plan)).isEmpty();
  }

  @Test
  void testJobNameSuffix() {
    when(plan.table()).thenReturn(table);
    when(table.name()).thenReturn("catalog.db.table");

    assertThat(builder.jobNameSuffix(plan)).hasValue("catalog.db.table");
    assertThat(builder.jobNameSuffixFromLogicalPlan(plan)).hasValue("catalog.db.table");
  }
}
