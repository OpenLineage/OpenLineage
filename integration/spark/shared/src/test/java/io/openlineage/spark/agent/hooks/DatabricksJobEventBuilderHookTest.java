/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.hooks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.util.DatabricksUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Collections;
import java.util.Optional;
import org.apache.spark.sql.execution.QueryExecution;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DatabricksJobEventBuilderHookTest {

  OpenLineageContext context = mock(OpenLineageContext.class);
  QueryExecution queryExecution = mock(QueryExecution.class, RETURNS_DEEP_STUBS);
  DatabricksJobEventBuilderHook builderHook = new DatabricksJobEventBuilderHook(context);
  OpenLineage.RunEventBuilder runEventBuilder;

  @BeforeEach
  public void setup() {
    when(context.getQueryExecution()).thenReturn(Optional.of(queryExecution));
    when(context.getOpenLineage()).thenReturn(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI));
    runEventBuilder = context.getOpenLineage().newRunEventBuilder();

    when(context
            .getQueryExecution()
            .get()
            .sparkSession()
            .sparkContext()
            .getConf()
            .contains(DatabricksUtils.SPARK_DATABRICKS_WORKSPACE_URL))
        .thenReturn(true);
    when(context
            .getQueryExecution()
            .get()
            .sparkSession()
            .sparkContext()
            .getConf()
            .get(DatabricksUtils.SPARK_DATABRICKS_WORKSPACE_URL))
        .thenReturn("https://dbc-954f5d5f-34dd.cloud.databricks.com/");
  }

  @Test
  public void testPreBuildWhenNotRunningOnDatabricksPlatform() {
    when(queryExecution
            .sparkSession()
            .sparkContext()
            .getConf()
            .contains(DatabricksUtils.SPARK_DATABRICKS_WORKSPACE_URL))
        .thenReturn(false);

    runEventBuilder = mock(OpenLineage.RunEventBuilder.class);
    builderHook.preBuild(runEventBuilder);
    verify(runEventBuilder, never()).job(any());
  }

  @Test
  public void testPreBuild() {
    runEventBuilder.job(
        context
            .getOpenLineage()
            .newJobBuilder()
            .name("databricks_shell.append_data_exec_v1")
            .build());
    runEventBuilder.outputs(
        Collections.singletonList(
            context
                .getOpenLineage()
                .newOutputDatasetBuilder()
                .namespace("dsnamespace")
                .name("/user/hive/warehouse/air_companies.db/air_companies")
                .build()));

    builderHook.preBuild(runEventBuilder);
    assertThat(runEventBuilder.build().getJob().getName())
        .isEqualTo("dbc-954f5d5f-34dd_append_data_exec_v1_air_companies_db_air_companies");
  }

  @Test
  public void testPreBuildWhenNoOutputDataset() {
    runEventBuilder.job(
        context.getOpenLineage().newJobBuilder().name("databricks_shell.append_data").build());

    builderHook.preBuild(runEventBuilder);
    assertThat(runEventBuilder.build().getJob().getName())
        .isEqualTo("dbc-954f5d5f-34dd_append_data");
  }

  @Test
  public void testPreBuildWhenNotADefaultName() {
    runEventBuilder.job(
        context.getOpenLineage().newJobBuilder().name("non-default-name.append_data").build());

    builderHook.preBuild(runEventBuilder);
    assertThat(runEventBuilder.build().getJob().getName())
        .isEqualTo("non-default-name_append_data");
  }
}
