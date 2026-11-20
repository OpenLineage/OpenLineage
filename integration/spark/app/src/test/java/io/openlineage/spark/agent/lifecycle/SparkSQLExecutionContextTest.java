/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
import io.openlineage.spark.agent.EventEmitter;
import io.openlineage.spark.agent.SparkAgentTestExtension;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.filters.EventFilterUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.CheckpointContext;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import io.openlineage.spark.api.OpenLineageRunStatus;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import io.openlineage.spark.api.VisitedNodes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.rdd.RDDOperationScope;
import org.apache.spark.scheduler.JobFailed;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import org.apache.spark.storage.RDDInfo;
import org.apache.spark.util.CallSite;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import scala.Option;

@ExtendWith(SparkAgentTestExtension.class)
class SparkSQLExecutionContextTest {

  private final long executionId = 1L;
  private final OpenLineageContext olContext = mock(OpenLineageContext.class);
  private final OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
  private final EventEmitter eventEmitter = mock(EventEmitter.class);
  private final SparkSQLExecutionContext context =
      new SparkSQLExecutionContext(
          executionId,
          eventEmitter,
          olContext,
          new OpenLineageRunEventBuilder(olContext, mock(OpenLineageEventHandlerFactory.class)));
  private final QueryExecution queryExecution = mock(QueryExecution.class, RETURNS_DEEP_STUBS);

  @AfterEach
  void reset() {
    Mockito.reset(olContext, eventEmitter, queryExecution);
  }

  @BeforeEach
  void setup(SparkSession spark) {
    when(olContext.getQueryExecution()).thenReturn(Optional.of(queryExecution));
    when(olContext.getSparkContext()).thenReturn(Optional.of(spark.sparkContext()));
    when(olContext.getLineageRunStatus()).thenReturn(new OpenLineageRunStatus());
    when(queryExecution.sparkPlan())
        .thenReturn(mock(org.apache.spark.sql.execution.SparkPlan.class));
    when(queryExecution.sparkPlan().sparkContext()).thenReturn(spark.sparkContext());
    when(olContext.getOpenLineage()).thenReturn(openLineage);
    when(olContext.getOpenLineageConfig()).thenReturn(new SparkOpenLineageConfig());
    when(olContext.getMeterRegistry()).thenReturn(new SimpleMeterRegistry());
    when(olContext.getVisitedNodes()).thenReturn(new VisitedNodes());
    when(olContext.getRunUuid())
        .thenReturn(UUID.fromString("8d99e33e-2a1c-4254-9600-18f23435fc3b"));

    when(eventEmitter.getOverriddenAppName()).thenReturn(Optional.of("test-rdd"));
    when(queryExecution.executedPlan().nodeName()).thenReturn("some-node-name");

    when(eventEmitter.getJobNamespace()).thenReturn("ns_name");
    when(eventEmitter.getApplicationRunId())
        .thenReturn(UUID.fromString("8d99e33e-bbbb-cccc-dddd-18f2343aaaaa"));
    when(eventEmitter.getApplicationJobName()).thenReturn("app-name");
  }

  @Test
  void testSingleStartIsSent(SparkSession spark) {
    ArgumentCaptor<RunEvent> lineageEvent = ArgumentCaptor.forClass(OpenLineage.RunEvent.class);
    try (MockedStatic<EventFilterUtils> ignored = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDisabled(any(), any())).thenReturn(false);
    }

    context.start(mock(SparkListenerSQLExecutionStart.class));
    context.start(mock(SparkListenerJobStart.class));
    verify(eventEmitter, times(2)).emit(lineageEvent.capture());

    assertThat(lineageEvent.getAllValues().get(0))
        .hasFieldOrPropertyWithValue("eventType", EventType.START);
    assertThat(lineageEvent.getAllValues().get(1))
        .hasFieldOrPropertyWithValue("eventType", EventType.RUNNING);
  }

  @Test
  void testSingleStartIsSentWhenJobStartGoesFirst(SparkSession spark) {
    ArgumentCaptor<RunEvent> lineageEvent = ArgumentCaptor.forClass(OpenLineage.RunEvent.class);
    try (MockedStatic<EventFilterUtils> ignored = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDisabled(any(), any())).thenReturn(false);

      context.start(mock(SparkListenerJobStart.class));
      context.start(mock(SparkListenerSQLExecutionStart.class));
    }
    verify(eventEmitter, times(2)).emit(lineageEvent.capture());

    assertThat(lineageEvent.getAllValues().get(0))
        .hasFieldOrPropertyWithValue("eventType", EventType.START);
    assertThat(lineageEvent.getAllValues().get(1))
        .hasFieldOrPropertyWithValue("eventType", EventType.RUNNING);
  }

  @Test
  void testSingleCompleteIsSent(SparkSession spark) {
    ArgumentCaptor<RunEvent> lineageEvent = ArgumentCaptor.forClass(OpenLineage.RunEvent.class);
    try (MockedStatic<EventFilterUtils> ignored = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDisabled(any(), any())).thenReturn(false);

      context.start(mock(SparkListenerJobStart.class));
      context.start(mock(SparkListenerSQLExecutionStart.class));
      context.end(mock(SparkListenerSQLExecutionEnd.class));
      context.end(mock(SparkListenerJobEnd.class));
    }
    verify(eventEmitter, times(4)).emit(lineageEvent.capture());

    assertThat(lineageEvent.getAllValues().get(2))
        .hasFieldOrPropertyWithValue("eventType", EventType.RUNNING);
    assertThat(lineageEvent.getAllValues().get(3))
        .hasFieldOrPropertyWithValue("eventType", EventType.COMPLETE);
  }

  @Test
  void testSingleCompleteIsSentWhenJobEndGoesFirst(SparkSession spark) {
    ArgumentCaptor<RunEvent> lineageEvent = ArgumentCaptor.forClass(OpenLineage.RunEvent.class);
    try (MockedStatic<EventFilterUtils> ignored = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDisabled(any(), any())).thenReturn(false);

      context.start(mock(SparkListenerJobStart.class));
      context.start(mock(SparkListenerSQLExecutionStart.class));
      context.end(mock(SparkListenerJobEnd.class));
      context.end(mock(SparkListenerSQLExecutionEnd.class));
    }
    verify(eventEmitter, times(4)).emit(lineageEvent.capture());

    assertThat(lineageEvent.getAllValues().get(2))
        .hasFieldOrPropertyWithValue("eventType", EventType.RUNNING);
    assertThat(lineageEvent.getAllValues().get(3))
        .hasFieldOrPropertyWithValue("eventType", EventType.COMPLETE);
  }

  @Test
  void testCompleteIsSentWhenNoSqlStart(SparkSession spark) {
    ArgumentCaptor<RunEvent> lineageEvent = ArgumentCaptor.forClass(OpenLineage.RunEvent.class);
    try (MockedStatic<EventFilterUtils> ignored = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDisabled(any(), any())).thenReturn(false);

      context.start(mock(SparkListenerJobStart.class));
      context.end(mock(SparkListenerJobEnd.class));
    }
    verify(eventEmitter, times(2)).emit(lineageEvent.capture());

    assertThat(lineageEvent.getAllValues().get(0))
        .hasFieldOrPropertyWithValue("eventType", EventType.START);
    assertThat(lineageEvent.getAllValues().get(1))
        .hasFieldOrPropertyWithValue("eventType", EventType.COMPLETE);
  }

  @Test
  void testFailIsSent(SparkSession spark) {
    ArgumentCaptor<RunEvent> lineageEvent = ArgumentCaptor.forClass(OpenLineage.RunEvent.class);
    SparkListenerJobEnd jobEnd = mock(SparkListenerJobEnd.class);
    when(jobEnd.jobId()).thenReturn(41);
    when(jobEnd.jobResult()).thenReturn(mock(JobFailed.class));
    try (MockedStatic<EventFilterUtils> ignored = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDisabled(any(), any())).thenReturn(false);

      context.start(mock(SparkListenerJobStart.class));
      context.end(jobEnd);
    }
    verify(eventEmitter, times(2)).emit(lineageEvent.capture());

    assertThat(lineageEvent.getAllValues().get(0))
        .hasFieldOrPropertyWithValue("eventType", EventType.START);
    assertThat(lineageEvent.getAllValues().get(1))
        .hasFieldOrPropertyWithValue("eventType", EventType.FAIL);
  }

  @Test
  void testSingleCompleteIsSentWithJobType(SparkSession spark) {
    ArgumentCaptor<RunEvent> lineageEvent = ArgumentCaptor.forClass(OpenLineage.RunEvent.class);
    try (MockedStatic<EventFilterUtils> ignored = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDisabled(any(), any())).thenReturn(false);

      context.start(mock(SparkListenerJobStart.class));
      context.start(mock(SparkListenerSQLExecutionStart.class));
      context.end(mock(SparkListenerSQLExecutionEnd.class));
      context.end(mock(SparkListenerJobEnd.class));
    }
    verify(eventEmitter, times(4)).emit(lineageEvent.capture());

    for (RunEvent event : lineageEvent.getAllValues()) {
      OpenLineage.JobTypeJobFacet jobType = event.getJob().getFacets().getJobType();
      assertThat(jobType).isNotNull();
      assertThat(jobType.getJobType()).isEqualTo("SQL_JOB");
      assertThat(jobType.getIntegration()).isEqualTo("SPARK");
      assertThat(jobType.getProcessingType()).isEqualTo("BATCH");
    }
  }

  @Test
  void testSingleCompleteIsSentWithJobTypeStreaming(SparkSession spark) {
    ArgumentCaptor<RunEvent> lineageEvent = ArgumentCaptor.forClass(OpenLineage.RunEvent.class);
    try (MockedStatic<EventFilterUtils> ignored = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDisabled(any(), any())).thenReturn(false);
      when(queryExecution.optimizedPlan().isStreaming()).thenReturn(true);

      context.start(mock(SparkListenerJobStart.class));
      context.start(mock(SparkListenerSQLExecutionStart.class));
      context.end(mock(SparkListenerSQLExecutionEnd.class));
      context.end(mock(SparkListenerJobEnd.class));
    }
    verify(eventEmitter, times(4)).emit(lineageEvent.capture());

    for (RunEvent event : lineageEvent.getAllValues()) {
      OpenLineage.JobTypeJobFacet jobType = event.getJob().getFacets().getJobType();
      assertThat(jobType).isNotNull();
      assertThat(jobType.getJobType()).isEqualTo("SQL_JOB");
      assertThat(jobType.getIntegration()).isEqualTo("SPARK");
      assertThat(jobType.getProcessingType()).isEqualTo("STREAMING");
    }
  }

  @Test
  void testRunAndJobAreSet(SparkSession spark) {
    ArgumentCaptor<RunEvent> lineageEvent = ArgumentCaptor.forClass(OpenLineage.RunEvent.class);
    try (MockedStatic<EventFilterUtils> ignored = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDisabled(any(), any())).thenReturn(false);

      context.start(mock(SparkListenerJobStart.class));
      context.start(mock(SparkListenerSQLExecutionStart.class));
      context.end(mock(SparkListenerSQLExecutionEnd.class));
      context.end(mock(SparkListenerJobEnd.class));
    }

    for (RunEvent runEvent : lineageEvent.getAllValues()) {
      OpenLineage.Run run = runEvent.getRun();
      OpenLineage.Job job = runEvent.getJob();
      assertThat(job.getName()).isEqualTo("test_rdd.some_node_name");
      assertThat(job.getNamespace()).isEqualTo("ns_name");
      assertThat(run.getRunId()).isEqualTo(UUID.fromString("8d99e33e-2a1c-4254-9600-18f23435fc3b"));
    }
  }

  @Test
  void testParentRunFacetIsSet(SparkSession spark) {
    ArgumentCaptor<RunEvent> lineageEvent = ArgumentCaptor.forClass(OpenLineage.RunEvent.class);
    try (MockedStatic<EventFilterUtils> ignored = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDisabled(any(), any())).thenReturn(false);
      when(olContext.getSparkContext()).thenReturn(Optional.of(spark.sparkContext()));

      context.start(mock(SparkListenerJobStart.class));
      context.start(mock(SparkListenerSQLExecutionStart.class));
      context.end(mock(SparkListenerSQLExecutionEnd.class));
      context.end(mock(SparkListenerJobEnd.class));
    }

    for (RunEvent runEvent : lineageEvent.getAllValues()) {
      OpenLineage.ParentRunFacet parentRunFacet = runEvent.getRun().getFacets().getParent();
      OpenLineage.ParentRunFacetJob parentJob = parentRunFacet.getJob();
      OpenLineage.ParentRunFacetRun parentRun = parentRunFacet.getRun();
      OpenLineage.RootJob rootJob = parentRunFacet.getRoot().getJob();
      OpenLineage.RootRun rootRun = parentRunFacet.getRoot().getRun();
      assertThat(parentJob.getName()).isEqualTo("app_name");
      assertThat(parentJob.getNamespace()).isEqualTo("ns_name");
      assertThat(parentRun.getRunId())
          .isEqualTo(UUID.fromString("8d99e33e-bbbb-cccc-dddd-18f2343aaaaa"));
      assertThat(rootJob.getName()).isEqualTo("app_name");
      assertThat(rootJob.getNamespace()).isEqualTo("ns_name");
      assertThat(rootRun.getRunId())
          .isEqualTo(UUID.fromString("8d99e33e-bbbb-cccc-dddd-18f2343aaaaa"));
    }
  }

  @Test
  void testParentRunFacetWithRootIsSet(SparkSession spark) {
    UUID rootUuid = UUID.randomUUID();
    ArgumentCaptor<RunEvent> lineageEvent = ArgumentCaptor.forClass(OpenLineage.RunEvent.class);
    try (MockedStatic<EventFilterUtils> ignored = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDisabled(any(), any())).thenReturn(false);
      when(olContext.getSparkContext()).thenReturn(Optional.of(spark.sparkContext()));
      when(eventEmitter.getRootParentRunId()).thenReturn(Optional.of(rootUuid));
      when(eventEmitter.getRootParentJobName()).thenReturn(Optional.of("root_job_name"));
      when(eventEmitter.getRootParentJobNamespace()).thenReturn(Optional.of("root_job_namespace"));

      context.start(mock(SparkListenerJobStart.class));
      context.start(mock(SparkListenerSQLExecutionStart.class));
      context.end(mock(SparkListenerSQLExecutionEnd.class));
      context.end(mock(SparkListenerJobEnd.class));
    }

    for (RunEvent runEvent : lineageEvent.getAllValues()) {
      OpenLineage.ParentRunFacet parentRunFacet = runEvent.getRun().getFacets().getParent();
      OpenLineage.ParentRunFacetJob parentJob = parentRunFacet.getJob();
      OpenLineage.ParentRunFacetRun parentRun = parentRunFacet.getRun();
      OpenLineage.RootJob rootJob = parentRunFacet.getRoot().getJob();
      OpenLineage.RootRun rootRun = parentRunFacet.getRoot().getRun();
      assertThat(parentJob.getName()).isEqualTo("app_name");
      assertThat(parentJob.getNamespace()).isEqualTo("ns_name");
      assertThat(parentRun.getRunId())
          .isEqualTo(UUID.fromString("8d99e33e-bbbb-cccc-dddd-18f2343aaaaa"));
      assertThat(rootJob.getName()).isEqualTo("root_job_name");
      assertThat(rootJob.getNamespace()).isEqualTo("root_job_namespace");
      assertThat(rootRun.getRunId()).isEqualTo(rootUuid);
    }
  }

  @Test
  void testCheckpointMethodIsResolvedFromOutermostSparkFrame() {
    assertThat(
            SparkSQLExecutionContext.checkpointMethod(
                "org.apache.spark.sql.Dataset.localCheckpoint(Dataset.scala:690)\n"
                    + "com.example.Job.main(Job.scala:42)"))
        .contains("localCheckpoint");
    assertThat(
            SparkSQLExecutionContext.checkpointMethod(
                "org.apache.spark.sql.classic.Dataset.checkpoint(Dataset.scala:1842)\n"
                    + "com.example.Job.main(Job.scala:42)"))
        .contains("checkpoint");
  }

  @Test
  void testCheckpointMethodIgnoresOtherCallSites() {
    assertThat(
            SparkSQLExecutionContext.checkpointMethod(
                "org.apache.spark.sql.DataFrameWriter.saveAsTable(DataFrameWriter.scala:633)\n"
                    + "com.example.Job.checkpoint(Job.scala:42)"))
        .isEmpty();
    assertThat(
            SparkSQLExecutionContext.checkpointMethod(
                "org.apache.spark.sql.delta.DeltaLog.checkpoint(DeltaLog.scala:100)\n"
                    + "com.example.Job.main(Job.scala:42)"))
        .isEmpty();
    assertThat(SparkSQLExecutionContext.checkpointMethod("checkpoint at Job.scala:42")).isEmpty();
    assertThat(SparkSQLExecutionContext.checkpointMethod(null)).isEmpty();
  }

  @Test
  void testCheckpointMaterializationJobIsDetectedByRddScope() {
    assertThat(SparkSQLExecutionContext.isCheckpointMaterializationJob(null)).isEmpty();
    assertThat(SparkSQLExecutionContext.isCheckpointMaterializationJob(new Properties()))
        .contains(false);
    assertThat(SparkSQLExecutionContext.isCheckpointMaterializationJob(scoped("checkpoint")))
        .contains(true);
    assertThat(SparkSQLExecutionContext.isCheckpointMaterializationJob(scoped("collect")))
        .contains(false);
    Properties malformed = new Properties();
    malformed.setProperty(RDD_SCOPE_PROPERTY, "not json");
    assertThat(SparkSQLExecutionContext.isCheckpointMaterializationJob(malformed)).contains(false);
  }

  @Test
  void testEagerLocalCheckpointIsResolvedFromItsMaterializationJob(SparkSession spark) {
    enableCheckpointLineage();
    // an unclaimed local checkpoint created at the same call site with a lower id, e.g. by another
    // thread running the same code concurrently
    when(olContext.getSparkContext()).thenReturn(Optional.of(spark.sparkContext()));
    List<RDD<Integer>> rdds = localCheckpointsAtCallSite(spark, 2);
    try {
      context.start(checkpointExecutionStart());
      context.start(jobStart(rdds.get(1).id(), scoped("checkpoint")));

      assertThat(context.resolveCheckpointRddId()).contains(rdds.get(1).id());
    } finally {
      rdds.forEach(rdd -> rdd.unpersist(true));
    }
  }

  @Test
  void testLocalCheckpointIgnoresJobsNotMaterializingIt(SparkSession spark) {
    enableCheckpointLineage();
    when(olContext.getSparkContext()).thenReturn(Optional.of(spark.sparkContext()));
    List<RDD<Integer>> rdds = localCheckpointsAtCallSite(spark, 1);
    try {
      context.start(checkpointExecutionStart());
      // e.g. a broadcast run while preparing the plan of a lazy checkpoint
      context.start(jobStart(rdds.get(0).id() + 100, scoped("collect")));

      assertThat(context.resolveCheckpointRddId()).contains(rdds.get(0).id());
    } finally {
      rdds.forEach(rdd -> rdd.unpersist(true));
    }
  }

  @Test
  void testNonLocalCheckpointIsResolvedFromItsMaterializationJobOnly() {
    enableCheckpointLineage();
    SparkListenerSQLExecutionStart start = mock(SparkListenerSQLExecutionStart.class);
    when(start.details()).thenReturn(NON_LOCAL_CHECKPOINT_CALL_SITE);
    context.start(start);

    // a lazy checkpoint runs no materialization job, only ones preparing its plan
    context.start(jobStart(42, scoped("collect")));
    assertThat(context.resolveCheckpointRddId()).isEmpty();

    context.start(jobStart(43, scoped("checkpoint")));
    assertThat(context.resolveCheckpointRddId()).contains(43);
  }

  private static final String RDD_SCOPE_PROPERTY = "spark.rdd.scope";
  private static final String LOCAL_CHECKPOINT_CALL_SITE =
      "org.apache.spark.sql.Dataset.localCheckpoint(Dataset.scala:690)\n"
          + "com.example.Job.checkpointTable(Job.scala:42)";
  private static final String NON_LOCAL_CHECKPOINT_CALL_SITE =
      "org.apache.spark.sql.Dataset.checkpoint(Dataset.scala:670)\n"
          + "com.example.Job.checkpointTable(Job.scala:42)";

  private void enableCheckpointLineage() {
    SparkOpenLineageConfig config = new SparkOpenLineageConfig();
    config.getCheckpointConfig().setEnabled(true);
    when(olContext.getOpenLineageConfig()).thenReturn(config);
    when(olContext.getCheckpointContext()).thenReturn(new CheckpointContext());
  }

  private static SparkListenerSQLExecutionStart checkpointExecutionStart() {
    SparkListenerSQLExecutionStart start = mock(SparkListenerSQLExecutionStart.class);
    when(start.details()).thenReturn(LOCAL_CHECKPOINT_CALL_SITE);
    return start;
  }

  /** Locally checkpointed RDDs created at {@link #LOCAL_CHECKPOINT_CALL_SITE}, in id order. */
  private static List<RDD<Integer>> localCheckpointsAtCallSite(SparkSession spark, int count) {
    SparkContext sc = spark.sparkContext();
    sc.setLocalProperty(CallSite.SHORT_FORM(), "localCheckpoint at Job.scala:42");
    sc.setLocalProperty(CallSite.LONG_FORM(), LOCAL_CHECKPOINT_CALL_SITE);
    try {
      List<RDD<Integer>> rdds = new ArrayList<>();
      for (int i = 0; i < count; i++) {
        RDD<Integer> rdd =
            JavaSparkContext.fromSparkContext(sc).parallelize(Arrays.asList(1, 2, 3)).rdd();
        rdd.localCheckpoint();
        rdds.add(rdd);
      }
      return rdds;
    } finally {
      sc.setLocalProperty(CallSite.SHORT_FORM(), null);
      sc.setLocalProperty(CallSite.LONG_FORM(), null);
    }
  }

  private static Properties scoped(String scopeName) {
    Properties properties = new Properties();
    properties.setProperty(
        RDD_SCOPE_PROPERTY, new RDDOperationScope(scopeName, Option.empty(), "1").toJson());
    return properties;
  }

  private static SparkListenerJobStart jobStart(int resultStageRddId, Properties properties) {
    RDDInfo rddInfo = mock(RDDInfo.class);
    when(rddInfo.id()).thenReturn(resultStageRddId);
    StageInfo stage = mock(StageInfo.class);
    when(stage.stageId()).thenReturn(1);
    when(stage.rddInfos()).thenReturn(ScalaConversionUtils.fromList(Arrays.asList(rddInfo)));
    SparkListenerJobStart jobStart = mock(SparkListenerJobStart.class);
    when(jobStart.stageInfos()).thenReturn(ScalaConversionUtils.fromList(Arrays.asList(stage)));
    when(jobStart.properties()).thenReturn(properties);
    return jobStart;
  }
}
