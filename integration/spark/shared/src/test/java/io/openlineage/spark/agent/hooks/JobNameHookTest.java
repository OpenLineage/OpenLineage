/*
/* Copyright 2018-2024 contributors to the OpenLineage project
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
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.util.Collections;
import java.util.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.execution.QueryExecution;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class JobNameHookTest {

  OpenLineageContext context = mock(OpenLineageContext.class);
  QueryExecution queryExecution = mock(QueryExecution.class, RETURNS_DEEP_STUBS);
  JobNameHook builderHook = new JobNameHook(context);
  SparkConf sparkConf = mock(SparkConf.class);
  SparkContext sparkContext = mock(SparkContext.class);
  SparkOpenLineageConfig config;
  OpenLineage.RunEventBuilder runEventBuilder;
  String jobName = null;

  @BeforeEach
  public void setup() {
    config = new SparkOpenLineageConfig();
    when(context.getQueryExecution()).thenReturn(Optional.of(queryExecution));
    when(context.getOpenLineage()).thenReturn(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI));
    when(context.getSparkContext()).thenReturn(sparkContext);
    when(context.getOpenLineageConfig()).thenReturn(config);
    when(sparkContext.conf()).thenReturn(sparkConf);

    config.getJobName().setAppendDatasetName(true);
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
    when(context.getJobName()).thenReturn(jobName);
  }

  @Test
  void testPreBuildWhenAppendingDatasetNameToJobNameDisabled() {
    config.getJobName().setAppendDatasetName(false);

    runEventBuilder = mock(OpenLineage.RunEventBuilder.class);
    builderHook.preBuild(runEventBuilder);
    verify(runEventBuilder, never()).job(any());
  }

  @Test
  void testPreBuild() {
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
        .isEqualTo("dbc-954f5d5f-34dd.append_data_exec_v1.air_companies_db_air_companies");
  }

  @Test
  void testPreBuildWhenReplaceDotWithUnderscoreIsTrue() {
    config.getJobName().setReplaceDotWithUnderscore(true);

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
  void testPreBuildWhenNoOutputDataset() {
    runEventBuilder.job(
        context.getOpenLineage().newJobBuilder().name("databricks_shell.append_data").build());

    builderHook.preBuild(runEventBuilder);
    assertThat(runEventBuilder.build().getJob().getName())
        .isEqualTo("dbc-954f5d5f-34dd.append_data");
  }

  @Test
  void testPreBuildWhenNotADefaultName() {
    runEventBuilder.job(
        context.getOpenLineage().newJobBuilder().name("non-default-name.append_data").build());

    builderHook.preBuild(runEventBuilder);
    assertThat(runEventBuilder.build().getJob().getName())
        .isEqualTo("non-default-name.append_data");
  }

  @Test
  void testPreBuildWhenJobNamePresentInContext() {
    when(context.getJobName()).thenReturn("some-job-name");
    runEventBuilder.job(context.getOpenLineage().newJobBuilder().build());
    builderHook.preBuild(runEventBuilder);
    assertThat(runEventBuilder.build().getJob().getName()).isEqualTo("some-job-name");
  }
}
