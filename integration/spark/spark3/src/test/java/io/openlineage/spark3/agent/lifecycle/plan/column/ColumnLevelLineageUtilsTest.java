/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageContext;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.ColumnLineageConfig;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ColumnLevelLineageUtilsTest {

  private OpenLineageContext olContext;
  private SparkOpenLineageConfig config;
  private ColumnLineageConfig columnLineageConfig;
  private OpenLineage.SchemaDatasetFacet schemaFacet;
  private SparkListenerEvent event;
  private QueryExecution queryExecution;

  @BeforeEach
  void setup() {
    olContext = mock(OpenLineageContext.class);
    config = mock(SparkOpenLineageConfig.class);
    columnLineageConfig = mock(ColumnLineageConfig.class);
    schemaFacet = mock(OpenLineage.SchemaDatasetFacet.class);
    event = mock(SparkListenerEvent.class);
    queryExecution = mock(QueryExecution.class);

    when(olContext.getOpenLineageConfig()).thenReturn(config);
    when(config.getColumnLineageConfig()).thenReturn(columnLineageConfig);
    when(olContext.getQueryExecution()).thenReturn(Optional.of(queryExecution));
    when(queryExecution.optimizedPlan()).thenReturn(mock(LogicalPlan.class));
  }

  @Test
  void testReturnsEmptyWhenQueryExecutionNotPresent() {
    when(olContext.getQueryExecution()).thenReturn(Optional.empty());

    Optional<OpenLineage.ColumnLineageDatasetFacet> result =
        ColumnLevelLineageUtils.buildColumnLineageDatasetFacet(event, olContext, schemaFacet);

    assertFalse(result.isPresent());
  }

  @Test
  void testReturnsEmptyWhenSchemaFacetIsNull() {
    // When
    Optional<OpenLineage.ColumnLineageDatasetFacet> result =
        ColumnLevelLineageUtils.buildColumnLineageDatasetFacet(event, olContext, null);

    // Then
    assertFalse(result.isPresent());
  }

  @Test
  void testReturnsEmptyWhenOptimizedPlanIsNull() {
    when(queryExecution.optimizedPlan()).thenReturn(null);

    // When
    Optional<OpenLineage.ColumnLineageDatasetFacet> result =
        ColumnLevelLineageUtils.buildColumnLineageDatasetFacet(event, olContext, schemaFacet);

    // Then
    assertFalse(result.isPresent());
  }

  @Test
  void testSchemaExceedsLimitLogic() {
    // Given: different schema sizes and limits
    when(columnLineageConfig.getSchemaSizeLimit()).thenReturn(100);

    List<OpenLineage.SchemaDatasetFacetFields> smallFields = new ArrayList<>();
    for (int i = 0; i < 50; i++) {
      smallFields.add(mock(OpenLineage.SchemaDatasetFacetFields.class));
    }
    when(schemaFacet.getFields()).thenReturn(smallFields);

    boolean smallSchemaExceedsLimit =
        schemaFacet.getFields().size() > columnLineageConfig.getSchemaSizeLimit();
    assertFalse(smallSchemaExceedsLimit, "Schema with 50 fields should not exceed limit of 100");

    List<OpenLineage.SchemaDatasetFacetFields> boundaryFields = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      boundaryFields.add(mock(OpenLineage.SchemaDatasetFacetFields.class));
    }
    when(schemaFacet.getFields()).thenReturn(boundaryFields);

    boolean boundarySchemaExceedsLimit =
        schemaFacet.getFields().size() > columnLineageConfig.getSchemaSizeLimit();
    assertFalse(
        boundarySchemaExceedsLimit,
        "Schema with exactly 100 fields should not exceed limit of 100");

    List<OpenLineage.SchemaDatasetFacetFields> largeFields = new ArrayList<>();
    for (int i = 0; i < 150; i++) {
      largeFields.add(mock(OpenLineage.SchemaDatasetFacetFields.class));
    }
    when(schemaFacet.getFields()).thenReturn(largeFields);

    boolean largeSchemaExceedsLimit =
        schemaFacet.getFields().size() > columnLineageConfig.getSchemaSizeLimit();
    assertTrue(largeSchemaExceedsLimit, "Schema with 150 fields should exceed limit of 100");
  }

  /**
   * Spark 4.1 serves {@code cached.select(a AS c).cache().select(c AS e)} from the first cache, via
   * an InMemoryRelation renamed to {@code e} over a cached plan whose output is {@code a}. The
   * columns only line up by position.
   */
  @Test
  void testMapCachedOutputsByPositionLinksRenamedColumns() {
    ColumnLevelLineageBuilder builder = mock(ColumnLevelLineageBuilder.class);
    ColumnLevelLineageContext context = mock(ColumnLevelLineageContext.class);
    when(context.getBuilder()).thenReturn(builder);

    Attribute e = attribute(1, "e");
    Attribute f = attribute(2, "f");
    Attribute a = attribute(3, "a");
    Attribute b = attribute(4, "b");

    ColumnLevelLineageUtils.mapCachedOutputsByPosition(
        context, planWithOutput(e, f), planWithOutput(a, b));

    verify(builder).addDependency(e.exprId(), a.exprId(), "e");
    verify(builder).addDependency(f.exprId(), b.exprId(), "f");
  }

  /** Outputs of different sizes cannot be paired up, so nothing is linked. */
  @Test
  void testMapCachedOutputsByPositionSkipsMismatchedOutputs() {
    ColumnLevelLineageBuilder builder = mock(ColumnLevelLineageBuilder.class);
    ColumnLevelLineageContext context = mock(ColumnLevelLineageContext.class);
    when(context.getBuilder()).thenReturn(builder);

    ColumnLevelLineageUtils.mapCachedOutputsByPosition(
        context,
        planWithOutput(attribute(1, "e")),
        planWithOutput(attribute(3, "a"), attribute(4, "b")));

    verifyNoInteractions(builder);
  }

  private static Attribute attribute(long id, String name) {
    Attribute attribute = mock(Attribute.class);
    when(attribute.exprId()).thenReturn(ExprId.apply(id));
    when(attribute.qualifiedName()).thenReturn(name);
    return attribute;
  }

  private static LogicalPlan planWithOutput(Attribute... attributes) {
    LogicalPlan plan = mock(LogicalPlan.class);
    when(plan.output()).thenReturn(ScalaConversionUtils.fromList(Arrays.asList(attributes)));
    return plan;
  }
}
