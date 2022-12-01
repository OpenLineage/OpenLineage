/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.filters;

import static io.openlineage.spark.agent.util.DatabricksUtils.SPARK_DATABRICKS_WORKSPACE_URL;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.spark.api.OpenLineageContext;
import java.util.Optional;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.WholeStageCodegenExec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DatabricksEventFilterTest {

  SparkListenerEvent event = mock(SparkListenerEvent.class);
  OpenLineageContext context = mock(OpenLineageContext.class);
  DatabricksEventFilter filter = new DatabricksEventFilter(context);
  QueryExecution queryExecution = mock(QueryExecution.class, RETURNS_DEEP_STUBS);
  WholeStageCodegenExec node = mock(WholeStageCodegenExec.class);

  @BeforeEach
  public void setup() {
    when(context.getQueryExecution()).thenReturn(Optional.of(queryExecution));
    when(queryExecution
            .sparkSession()
            .sparkContext()
            .getConf()
            .contains(SPARK_DATABRICKS_WORKSPACE_URL))
        .thenReturn(true);
    when(queryExecution.sparkSession().sparkContext().getConf().get(SPARK_DATABRICKS_WORKSPACE_URL))
        .thenReturn("some-url");
    when(queryExecution.executedPlan()).thenReturn(node);
    when((node).child()).thenReturn(node);
  }

  @Test
  public void testDatabricksEventIsFiltered() {
    when(node.nodeName()).thenReturn("collect_Limit");
    assertThat(filter.isDisabled(event)).isTrue();
  }

  @Test
  public void testDatabricksEventIsFilteredWithoutUnderscore() {
    when(node.nodeName()).thenReturn("shownamespaces");
    assertThat(filter.isDisabled(event)).isTrue();
  }

  @Test
  public void testDatabricksEventIsNotFiltered() {
    when(node.nodeName()).thenReturn("action_not_to_be_filtered");
    assertThat(filter.isDisabled(event)).isFalse();
  }

  @Test
  public void testOtherEventIsNotFiltered() {
    when(queryExecution
            .sparkSession()
            .sparkContext()
            .getConf()
            .contains("spark.databricks.workspaceUrl"))
        .thenReturn(false);
    when(node.nodeName()).thenReturn("collect_Limit");
    assertThat(filter.isDisabled(event)).isFalse();
  }
}
