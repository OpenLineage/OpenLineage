/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.Join;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.delta.commands.MergeIntoCommand;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.runtime.AbstractPartialFunction;

class MergeIntoCommandInputDatasetBuilderTest {
  private static final OpenLineage OL = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
  private final OpenLineageContext context = mock(OpenLineageContext.class);
  private final SparkListenerEvent event = new SparkListenerSQLExecutionEnd(1L, 1L);
  private final Map<LogicalPlan, List<InputDataset>> resolved = new IdentityHashMap<>();
  private final LogicalRelation target = mock(LogicalRelation.class);

  @BeforeEach
  void setup() {
    resolved.put(target, Collections.singletonList(dataset("target")));
    when(context.getInputDatasetBuilders()).thenReturn(Collections.emptyList());
    when(context.getInputDatasetQueryPlanVisitors())
        .thenReturn(
            Collections.singletonList(
                new AbstractPartialFunction<LogicalPlan, List<InputDataset>>() {
                  @Override
                  public boolean isDefinedAt(LogicalPlan plan) {
                    return resolved.containsKey(plan);
                  }

                  @Override
                  public List<InputDataset> apply(LogicalPlan plan) {
                    return resolved.get(plan);
                  }
                }));
  }

  @Test
  void collectsBothJoinBranchesThroughWrappers() {
    LogicalPlan left = node(Filter.class, node(Aggregate.class, relation("left")));
    LogicalPlan right = node(Project.class, relation("right"));
    assertInputs(node(Join.class, left, right), "target", "left", "right");
  }

  @Test
  void keepsPlainSource() {
    assertInputs(relation("source"), "target", "source");
  }

  @Test
  void stopsAtResolvedParent() {
    LogicalPlan parent = node(Project.class, relation("source"));
    resolved.put(parent, Collections.singletonList(dataset("source")));
    assertInputs(parent, "target", "source");
  }

  @Test
  void ignoresUnresolvedSource() {
    assertInputs(node(LogicalPlan.class), "target");
  }

  @Test
  void edgeUsesTheSameTraversal() {
    LogicalPlan source = node(Filter.class, relation("source"));
    EdgeCommand command = mock(EdgeCommand.class);
    when(command.target()).thenReturn(target);
    when(command.source()).thenReturn(source);
    assertThat(new MergeIntoCommandEdgeInputDatasetBuilder(context).apply(event, command))
        .extracting(InputDataset::getName)
        .containsExactly("target", "source");
  }

  private void assertInputs(LogicalPlan source, String... names) {
    MergeIntoCommand command = mock(MergeIntoCommand.class);
    when(command.target()).thenReturn(target);
    when(command.source()).thenReturn(source);
    assertThat(new MergeIntoCommandInputDatasetBuilder(context).apply(event, command))
        .extracting(InputDataset::getName)
        .containsExactly(names);
  }

  private LogicalPlan relation(String name) {
    LogicalPlan plan = node(LogicalRelation.class);
    resolved.put(plan, Collections.singletonList(dataset(name)));
    return plan;
  }

  private LogicalPlan node(Class<? extends LogicalPlan> type, LogicalPlan... children) {
    LogicalPlan plan = mock(type);
    when(plan.children()).thenReturn(ScalaConversionUtils.fromList(Arrays.asList(children)));
    return plan;
  }

  private InputDataset dataset(String name) {
    return OL.newInputDatasetBuilder().name(name).namespace("file").build();
  }

  /** Reflective fixture only; does not model the Databricks runtime. */
  public abstract static class EdgeCommand extends LogicalPlan {
    public abstract LogicalPlan target();

    public abstract LogicalPlan source();
  }
}
