/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api.naming;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.util.DatabricksUtils;
import io.openlineage.spark.api.JobNameSuffixProvider;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.util.Collections;
import java.util.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.execution.QueryExecution;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import scala.PartialFunction;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class JobNameBuilderTest {

  OpenLineageContext context = mock(OpenLineageContext.class);
  QueryExecution queryExecution = mock(QueryExecution.class, RETURNS_DEEP_STUBS);
  SparkConf sparkConf = mock(SparkConf.class);
  SparkContext sparkContext = mock(SparkContext.class);
  SparkOpenLineageConfig config;
  String jobName = null;
  PartialFunction datasetBuilder =
      mock(PartialFunction.class, withSettings().extraInterfaces(JobNameSuffixProvider.class));

  @BeforeEach
  public void setup() {
    config = new SparkOpenLineageConfig();
    when(context.getQueryExecution()).thenReturn(Optional.of(queryExecution));
    when(context.getOpenLineage()).thenReturn(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI));
    when(context.getSparkContext()).thenReturn(Optional.of(sparkContext));
    when(context.getOpenLineageConfig()).thenReturn(config);
    when(sparkContext.getConf()).thenReturn(sparkConf);

    config.getJobName().setAppendDatasetName(true);

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
  void testBuildWhenOverriddenAppNameIsPresent() {
    config.setOverriddenAppName("some-overridden-name");
    assertThat(JobNameBuilder.build(context)).isEqualTo("some_overridden_name");
  }

  @Test
  void testBuildNormalizesName() {
    config.setOverriddenAppName("SomeOverriddenName");
    assertThat(JobNameBuilder.build(context)).isEqualTo("some_overridden_name");

    config.setOverriddenAppName("OLT-58");
    assertThat(JobNameBuilder.build(context)).isEqualTo("olt_58");

    config.setOverriddenAppName("Some job (Like from James)");
    assertThat(JobNameBuilder.build(context)).isEqualTo("some_job_like_from_james");
  }

  @Test
  void testSparkNodeNameIsAddedToJobName() {
    when(context.getQueryExecution().get().executedPlan().nodeName())
        .thenReturn("execute_CreateTable");
    when(sparkContext.appName()).thenReturn("spark_app");
    assertThat(JobNameBuilder.build(context)).isEqualTo("spark_app.execute_create_table");
  }

  @Test
  void testBuildIsNotRunWhenContextHasJobNameSet() {
    when(context.getQueryExecution().get().executedPlan().nodeName()).thenReturn("some_node");
    when(sparkContext.appName()).thenReturn("spark_app");
    assertThat(JobNameBuilder.build(context)).isEqualTo("spark_app.some_node");

    verify(context, times(1)).setJobName("spark_app.some_node");

    // verify job name is set
    when(context.getJobName()).thenReturn("other_job_name");
    assertThat(JobNameBuilder.build(context)).isEqualTo("other_job_name");
  }

  @Test
  void testBuildWithOutputDataset() {
    when(((JobNameSuffixProvider) datasetBuilder).jobNameSuffix(context))
        .thenReturn(Optional.of("air_companies_db_air_companies"));

    when(context.getQueryExecution().get().executedPlan().nodeName())
        .thenReturn("append_data_exec_v1");
    when(sparkContext.appName()).thenReturn("databricks_shell");
    when(context.getOutputDatasetBuilders()).thenReturn(Collections.singletonList(datasetBuilder));
    assertThat(JobNameBuilder.build(context))
        .isEqualTo("databricks_shell.append_data_exec_v1.air_companies_db_air_companies");
  }

  @Test
  void testBuildWhenAppendingDatasetNameToJobNameDisabled() {
    config.getJobName().setAppendDatasetName(false);
    when(((JobNameSuffixProvider) datasetBuilder).jobNameSuffix(context))
        .thenReturn(Optional.of("air_companies_db_air_companies"));

    when(context.getQueryExecution().get().executedPlan().nodeName())
        .thenReturn("append_data_exec_v1");
    when(sparkContext.appName()).thenReturn("databricks_shell");
    when(context.getOutputDatasetBuilders()).thenReturn(Collections.singletonList(datasetBuilder));
    assertThat(JobNameBuilder.build(context)).isEqualTo("databricks_shell.append_data_exec_v1");
  }

  @Test
  void testBuildWhenReplaceDotWithUnderscoreIsTrue() {
    config.getJobName().setReplaceDotWithUnderscore(true);
    when(((JobNameSuffixProvider) datasetBuilder).jobNameSuffix(context))
        .thenReturn(Optional.of("air_companies.db.air_companies"));

    when(context.getQueryExecution().get().executedPlan().nodeName())
        .thenReturn("append_data.exec_v1");
    when(sparkContext.appName()).thenReturn("databricks_shell");
    when(context.getOutputDatasetBuilders()).thenReturn(Collections.singletonList(datasetBuilder));
    assertThat(JobNameBuilder.build(context))
        .isEqualTo("databricks_shell.append_data_exec_v1.air_companies_db_air_companies");
  }

  @Test
  void testBuildPrettifiesDatabricksJobName() {
    try (MockedStatic mocked = mockStatic(DatabricksUtils.class)) {
      when(DatabricksUtils.isRunOnDatabricksPlatform(any(SparkConf.class))).thenReturn(true);
      when(DatabricksUtils.prettifyDatabricksJobName(any(SparkConf.class), any(String.class)))
          .thenReturn("dbc-954f5d5f-34dd.append_data.exec_v1");

      assertThat(JobNameBuilder.build(context)).isEqualTo("dbc-954f5d5f-34dd.append_data.exec_v1");
    }
  }
}
