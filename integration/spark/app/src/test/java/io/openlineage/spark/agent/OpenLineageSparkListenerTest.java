/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.Environment;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.circuitBreaker.NoOpCircuitBreaker;
import io.openlineage.spark.agent.filters.EventFilterUtils;
import io.openlineage.spark.agent.lifecycle.ContextFactory;
import io.openlineage.spark.agent.lifecycle.ExecutionContext;
import io.openlineage.spark.agent.lifecycle.SparkOpenLineageExtensionVisitorWrapper;
import io.openlineage.spark.agent.lifecycle.StaticExecutionContextFactory;
import io.openlineage.spark.agent.lifecycle.plan.InsertIntoHadoopFsRelationVisitor;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.UUID;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation$;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.SparkPlanInfo;
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import scala.Option;

class OpenLineageSparkListenerTest {

  SparkSession sparkSession = mock(SparkSession.class);
  SparkContext sparkContext = mock(SparkContext.class);
  EventEmitter emitter = mock(EventEmitter.class);
  QueryExecution qe = mock(QueryExecution.class);
  SparkPlan plan = mock(SparkPlan.class);
  SparkConf sparkConf = new SparkConf();

  OpenLineageContext olContext;

  @BeforeEach
  void setup() {
    when(sparkSession.sparkContext()).thenReturn(sparkContext);
    when(sparkContext.appName()).thenReturn("appName");
    when(sparkContext.getConf()).thenReturn(new SparkConf());
    when(plan.sparkContext()).thenReturn(sparkContext);
    when(plan.nodeName()).thenReturn("execute");

    when(emitter.getJobNamespace()).thenReturn("ns_name");
    when(emitter.getParentJobName()).thenReturn(Optional.of("parent_name"));
    when(emitter.getParentJobNamespace()).thenReturn(Optional.of("parent_namespace"));
    when(emitter.getParentRunId())
        .thenReturn(Optional.of(UUID.fromString("8d99e33e-2a1c-4254-9600-18f23435fc3b")));
    when(emitter.getApplicationRunId())
        .thenReturn(UUID.fromString("8d99e33e-bbbb-cccc-dddd-18f2343aaaaa"));
    when(emitter.getApplicationJobName()).thenReturn("test_rdd");

    SparkOpenLineageConfig openLineageConfig = new SparkOpenLineageConfig();
    olContext =
        OpenLineageContext.builder()
            .sparkSession(sparkSession)
            .sparkContext(sparkSession.sparkContext())
            .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
            .queryExecution(qe)
            .meterRegistry(new SimpleMeterRegistry())
            .openLineageConfig(openLineageConfig)
            .sparkExtensionVisitorWrapper(
                new SparkOpenLineageExtensionVisitorWrapper(openLineageConfig))
            .build();
  }

  @Test
  void testSqlEventWithJobEventEmitsOnce() {
    LogicalPlan query = UnresolvedRelation$.MODULE$.apply(TableIdentifier.apply("tableName"));

    when(qe.optimizedPlan())
        .thenReturn(
            new InsertIntoHadoopFsRelationCommand(
                new Path("file:///tmp/dir"),
                null,
                false,
                ScalaConversionUtils.asScalaSeqEmpty(),
                Option.empty(),
                null,
                ScalaConversionUtils.asScalaMapEmpty(),
                query,
                SaveMode.Overwrite,
                Option.empty(),
                Option.empty(),
                ScalaConversionUtils.<String>asScalaSeqEmpty()));

    when(qe.executedPlan()).thenReturn(plan);

    olContext
        .getOutputDatasetQueryPlanVisitors()
        .add(new InsertIntoHadoopFsRelationVisitor(olContext));
    ExecutionContext executionContext =
        new StaticExecutionContextFactory(
                emitter, new SimpleMeterRegistry(), new SparkOpenLineageConfig())
            .createSparkSQLExecutionContext(1L, emitter, qe, olContext);

    SparkListenerSQLExecutionStart event = mock(SparkListenerSQLExecutionStart.class);
    when(event.sparkPlanInfo())
        .thenReturn(
            new SparkPlanInfo(
                "name",
                "string",
                ScalaConversionUtils.asScalaSeqEmpty(),
                ScalaConversionUtils.asScalaMapEmpty(),
                ScalaConversionUtils.asScalaSeqEmpty()));
    when(event.executionId()).thenReturn(1L);
    try (MockedStatic<EventFilterUtils> utils = mockStatic(EventFilterUtils.class)) {
      utils.when(() -> EventFilterUtils.isDisabled(olContext, event)).thenReturn(false);
      executionContext.start(event);
    }

    ArgumentCaptor<OpenLineage.RunEvent> lineageEvent =
        ArgumentCaptor.forClass(OpenLineage.RunEvent.class);

    verify(emitter, times(1)).emit(lineageEvent.capture());
  }

  @Test
  void testOpenLineageDisableDisablesExecution() throws URISyntaxException {
    try (MockedStatic mocked = mockStatic(Environment.class)) {
      when(Environment.getEnvironmentVariable("OPENLINEAGE_DISABLED")).thenReturn("true");

      ContextFactory contextFactory = mock(ContextFactory.class);
      when(contextFactory.getMeterRegistry()).thenReturn(new SimpleMeterRegistry());

      OpenLineageSparkListener.init(contextFactory);
      OpenLineageSparkListener listener = new OpenLineageSparkListener(sparkConf);

      listener.onApplicationStart(mock(SparkListenerApplicationStart.class));
      listener.onApplicationEnd(mock(SparkListenerApplicationEnd.class));
      listener.onJobStart(mock(SparkListenerJobStart.class));
      listener.onJobEnd(mock(SparkListenerJobEnd.class));
      listener.onTaskEnd(mock(SparkListenerTaskEnd.class));
      listener.onOtherEvent(mock(SparkListenerSQLExecutionStart.class));
      listener.onOtherEvent(mock(SparkListenerSQLExecutionEnd.class));

      verify(contextFactory, never())
          .createSparkApplicationExecutionContext(sparkContext, new NoOpCircuitBreaker());
      verify(contextFactory, never()).createSparkSQLExecutionContext(anyLong());
    }
  }

  @Test
  void testSparkSQLEndGetsQueryExecutionFromEvent() {
    LogicalPlan query = UnresolvedRelation$.MODULE$.apply(TableIdentifier.apply("tableName"));

    when(sparkSession.sparkContext()).thenReturn(sparkContext);
    when(sparkContext.appName()).thenReturn("appName");
    when(sparkContext.getConf()).thenReturn(new SparkConf());
    when(qe.optimizedPlan())
        .thenReturn(
            new InsertIntoHadoopFsRelationCommand(
                new Path("file:///tmp/dir"),
                null,
                false,
                ScalaConversionUtils.asScalaSeqEmpty(),
                Option.empty(),
                null,
                ScalaConversionUtils.asScalaMapEmpty(),
                query,
                SaveMode.Overwrite,
                Option.empty(),
                Option.empty(),
                ScalaConversionUtils.asScalaSeqEmpty()));

    when(qe.executedPlan()).thenReturn(plan);
    when(qe.sparkSession()).thenReturn(sparkSession);
    when(plan.sparkContext()).thenReturn(sparkContext);
    when(plan.nodeName()).thenReturn("execute");

    olContext
        .getOutputDatasetQueryPlanVisitors()
        .add(new InsertIntoHadoopFsRelationVisitor(olContext));
    OpenLineageSparkListener listener = new OpenLineageSparkListener(sparkConf);
    OpenLineageSparkListener.init(
        new StaticExecutionContextFactory(
            emitter, new SimpleMeterRegistry(), new SparkOpenLineageConfig()));

    SparkListenerSQLExecutionEnd event = mock(SparkListenerSQLExecutionEnd.class);
    try (MockedStatic<EventFilterUtils> utils = mockStatic(EventFilterUtils.class)) {
      try (MockedStatic<ContextFactory> contextFactory =
          mockStatic(ContextFactory.class, Mockito.CALLS_REAL_METHODS)) {
        utils.when(() -> EventFilterUtils.isDisabled(olContext, event)).thenReturn(false);
        contextFactory
            .when(() -> ContextFactory.executionFromCompleteEvent(event))
            .thenReturn(Optional.of(qe)); // code should fail without this line
        listener.onOtherEvent(event);
      }
    }

    ArgumentCaptor<OpenLineage.RunEvent> lineageEvent =
        ArgumentCaptor.forClass(OpenLineage.RunEvent.class);

    verify(emitter, times(1)).emit(lineageEvent.capture());
  }

  @Test
  void testApplicationStartEvent() {
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    OpenLineageSparkListener listener = new OpenLineageSparkListener(sparkConf);
    OpenLineageSparkListener.init(
        new StaticExecutionContextFactory(emitter, meterRegistry, new SparkOpenLineageConfig()));
    SparkListenerApplicationStart event = mock(SparkListenerApplicationStart.class);

    listener.onApplicationStart(event);

    ArgumentCaptor<OpenLineage.RunEvent> lineageEvent =
        ArgumentCaptor.forClass(OpenLineage.RunEvent.class);

    verify(emitter, times(1)).emit(lineageEvent.capture());
  }

  @Test
  void testApplicationEndEvent() {
    OpenLineageSparkListener listener = new OpenLineageSparkListener(sparkConf);
    OpenLineageSparkListener.init(
        new StaticExecutionContextFactory(
            emitter, new SimpleMeterRegistry(), new SparkOpenLineageConfig()));
    SparkListenerApplicationEnd event = mock(SparkListenerApplicationEnd.class);

    listener.onApplicationEnd(event);

    ArgumentCaptor<OpenLineage.RunEvent> lineageEvent =
        ArgumentCaptor.forClass(OpenLineage.RunEvent.class);

    verify(emitter, times(1)).emit(lineageEvent.capture());
  }

  @Test
  void testCheckSparkApplicationEventsAreEmitted() {
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    OpenLineageSparkListener listener = new OpenLineageSparkListener(sparkConf);
    OpenLineageSparkListener.init(
        new StaticExecutionContextFactory(emitter, meterRegistry, new SparkOpenLineageConfig()));
    SparkListenerApplicationStart startEvent = mock(SparkListenerApplicationStart.class);
    SparkListenerApplicationEnd endEvent = mock(SparkListenerApplicationEnd.class);

    listener.onApplicationStart(startEvent);
    listener.onApplicationEnd(endEvent);

    assertThat(meterRegistry.counter("openlineage.spark.event.app.start").count()).isEqualTo(1.0);
    assertThat(meterRegistry.counter("openlineage.spark.event.app.end").count()).isEqualTo(1.0);
  }
}
