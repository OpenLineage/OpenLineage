/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.spark.api.OpenLineageContext;
import java.util.Optional;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.junit.jupiter.api.Test;

class SubqueryAliasOutputDatasetBuilderTest {

  private final OpenLineageContext context = mock(OpenLineageContext.class);
  private final SubqueryAliasOutputDatasetBuilder builder =
      new SubqueryAliasOutputDatasetBuilder(context);

  @Test
  void testQueryRootIsNotAnOutput() {
    SubqueryAlias alias = mock(SubqueryAlias.class);
    when(context.getOptimizedPlanOptional()).thenReturn(Optional.of(alias));
    when(context.getAnalyzedPlanOptional()).thenReturn(Optional.of(alias));

    assertFalse(builder.isDefinedAtLogicalPlan(alias));
  }

  @Test
  void testNestedAliasCanBeAnOutput() {
    SubqueryAlias alias = mock(SubqueryAlias.class);
    when(context.getOptimizedPlanOptional()).thenReturn(Optional.of(mock(LogicalPlan.class)));
    when(context.getAnalyzedPlanOptional()).thenReturn(Optional.of(mock(LogicalPlan.class)));

    assertTrue(builder.isDefinedAtLogicalPlan(alias));
  }
}
