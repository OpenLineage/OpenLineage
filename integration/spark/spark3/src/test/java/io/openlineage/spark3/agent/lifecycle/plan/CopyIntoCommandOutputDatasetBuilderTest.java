/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;

import io.openlineage.spark.api.OpenLineageContext;
import org.apache.spark.sql.catalyst.plans.logical.AppendData;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.junit.jupiter.api.Test;

class CopyIntoCommandOutputDatasetBuilderTest {

  OpenLineageContext context = mock(OpenLineageContext.class);
  CopyIntoCommandOutputDatasetBuilder builder = new CopyIntoCommandOutputDatasetBuilder(context);

  @Test
  void testIsNotDefinedForNonCopyIntoPlans() {
    assertFalse(builder.isDefinedAtLogicalPlan(mock(LogicalPlan.class)));
    assertFalse(builder.isDefinedAtLogicalPlan(mock(AppendData.class)));
  }
}
