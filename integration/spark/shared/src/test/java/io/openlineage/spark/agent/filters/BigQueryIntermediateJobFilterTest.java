/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.filters;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.spark.api.OpenLineageContext;
import java.util.Optional;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand;
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Option;

class BigQueryIntermediateJobFilterTest {

  OpenLineageContext context = mock(OpenLineageContext.class);
  BigQueryIntermediateJobFilter filter = new BigQueryIntermediateJobFilter(context);
  SparkSession sparkSession = mock(SparkSession.class);
  SparkContext sparkContext = mock(SparkContext.class);
  SparkConf sparkConf = mock(SparkConf.class);
  SparkListenerEvent sparkListenerEvent = mock(SparkListenerEvent.class);
  QueryExecution queryExecution = mock(QueryExecution.class);
  InsertIntoHadoopFsRelationCommand insert = mock(InsertIntoHadoopFsRelationCommand.class);

  String validPath = "gs://bucket/.spark-bigquery-local-123e4567-e89b-12d3-a456-426614174000";

  @BeforeEach
  public void setup() {
    when(sparkConf.getOption("temporaryGcsBucket")).thenReturn(Option.apply("bucket"));
    when(sparkSession.sparkContext()).thenReturn(sparkContext);
    when(sparkContext.conf()).thenReturn(sparkConf);
    when(sparkContext.getConf()).thenReturn(sparkConf);
    when(context.getQueryExecution()).thenReturn(Optional.of(queryExecution));
    when(context.getSparkContext()).thenReturn(Optional.of(sparkContext));
  }

  @Test
  void testIsDisabledWhenNoLogicalPlanPresent() {
    assertThat(filter.isDisabled(sparkListenerEvent)).isFalse();
  }

  @Test
  void testIsDisabledWhenForNonInsertIntoHadoopCommand() {
    when(queryExecution.optimizedPlan()).thenReturn(mock(SaveIntoDataSourceCommand.class));
    assertThat(filter.isDisabled(sparkListenerEvent)).isFalse();
  }

  @Test
  void testIsDisabledForNonGoogleStorage() {
    when(queryExecution.optimizedPlan()).thenReturn(insert);
    when(insert.outputPath()).thenReturn(new Path(validPath.replace("gs://", "s3://")));
    assertThat(filter.isDisabled(sparkListenerEvent)).isFalse();
  }

  @Test
  void testIsDisabledForInvalidUuidSuffix() {
    when(queryExecution.optimizedPlan()).thenReturn(insert);
    when(insert.outputPath()).thenReturn(new Path(validPath + "invalid-suffix"));
    assertThat(filter.isDisabled(sparkListenerEvent)).isFalse();
  }

  @Test
  void testIsDisabledForShortPath() {
    when(queryExecution.optimizedPlan()).thenReturn(insert);
    when(insert.outputPath()).thenReturn(new Path(validPath.substring(0, validPath.length() - 36)));
    assertThat(filter.isDisabled(sparkListenerEvent)).isFalse();
  }

  @Test
  void isDisabled() {
    when(queryExecution.optimizedPlan()).thenReturn(insert);
    when(insert.outputPath()).thenReturn(new Path(validPath));
    assertThat(filter.isDisabled(sparkListenerEvent)).isTrue();
  }

  @Test
  void isDisabledForPersistentGcsBucket() {
    when(queryExecution.optimizedPlan()).thenReturn(insert);
    when(insert.outputPath()).thenReturn(new Path(validPath));
    when(sparkConf.getOption("temporaryGcsBucket")).thenReturn(Option.empty());
    when(sparkConf.getOption("persistentGcsBucket")).thenReturn(Option.apply("bucket"));
    assertThat(filter.isDisabled(sparkListenerEvent)).isTrue();
  }
}
