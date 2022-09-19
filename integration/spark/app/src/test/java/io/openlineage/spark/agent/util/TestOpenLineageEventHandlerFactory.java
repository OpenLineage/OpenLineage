/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static org.mockito.Mockito.mock;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunFacet;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand;
import scala.PartialFunction;

/**
 * Test implementation - writes a custom {@link TestRunFacet} for every {@link SparkListenerJobEnd}
 * event.
 */
@Slf4j
public class TestOpenLineageEventHandlerFactory implements OpenLineageEventHandlerFactory {

  public static final String TEST_FACET_KEY = "test_event_handler_factory_run_facet";
  public static final String FAILING_TABLE_NAME_FAIL_ON_APPLY = "failing_table_apply";
  public static final String FAILING_TABLE_NAME_FAIL_ON_IS_DEFINED = "failing_table_is_defined";

  /**
   * Query plan visitor for {@link CreateDataSourceTableAsSelectCommand} that throws
   * java.lang.RuntimeException for table named
   * io.openlineage.spark.agent.util.TestOpenLineageEventHandlerFactory#FAILING_TABLE_NAME
   */
  QueryPlanVisitor failingCreateDataSourceTableAsSelectVisitor =
      new QueryPlanVisitor<CreateDataSourceTableAsSelectCommand, OpenLineage.InputDataset>(
          mock(OpenLineageContext.class)) {
        @Override
        public boolean isDefinedAt(LogicalPlan x) {
          if (!(x instanceof CreateDataSourceTableAsSelectCommand)) {
            return false;
          }

          String tableName =
              ((CreateDataSourceTableAsSelectCommand) x).table().identifier().table();

          if (FAILING_TABLE_NAME_FAIL_ON_APPLY.equals(tableName)) {
            return true;
          } else if (FAILING_TABLE_NAME_FAIL_ON_IS_DEFINED.equals(tableName)) {
            throw new RuntimeException(
                "Failing CreateDataSourceTableAsSelectCommand on isDefinedAt method");
          } else {
            return false;
          }
        }

        @Override
        public List<OpenLineage.InputDataset> apply(LogicalPlan x) {
          throw new RuntimeException(
              "Failing CreateDataSourceTableAsSelectCommand on apply method");
        }
      };

  @Override
  public List<CustomFacetBuilder<?, ? extends RunFacet>> createRunFacetBuilders(
      OpenLineageContext context) {
    return Collections.singletonList(new TestRunFacetBuilder());
  }

  @Getter
  public static class TestRunFacet extends OpenLineage.DefaultRunFacet {
    private final String message;

    public TestRunFacet(String message) {
      super(Versions.OPEN_LINEAGE_PRODUCER_URI);
      this.message = message;
    }
  }

  @Override
  public Collection<PartialFunction<LogicalPlan, List<OpenLineage.InputDataset>>>
      createInputDatasetQueryPlanVisitors(OpenLineageContext context) {
    return Collections.singletonList(failingCreateDataSourceTableAsSelectVisitor);
  }

  public static class TestRunFacetBuilder
      extends CustomFacetBuilder<SparkListenerJobEnd, TestRunFacet> {

    @Override
    protected void build(
        SparkListenerJobEnd event, BiConsumer<String, ? super TestRunFacet> consumer) {
      consumer.accept(TEST_FACET_KEY, new TestRunFacet(String.valueOf(event.jobId())));
    }
  }
}
