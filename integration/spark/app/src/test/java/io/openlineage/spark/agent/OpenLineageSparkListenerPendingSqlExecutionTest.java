/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.util.TimeUtils.toZonedTime;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.spark.agent.filters.EventFilterUtils;
import io.openlineage.spark.agent.lifecycle.ContextFactory;
import io.openlineage.spark.agent.lifecycle.ExecutionContext;
import io.openlineage.spark.agent.lifecycle.SparkOpenLineageExtensionVisitorWrapper;
import io.openlineage.spark.agent.lifecycle.StaticExecutionContextFactory;
import io.openlineage.spark.agent.lifecycle.plan.InsertIntoHadoopFsRelationVisitor;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.MockedStatic;
import scala.Option;

/**
 * Tests how {@link OpenLineageSparkListener} handles SQL executions whose start callback is
 * processed after Spark removed the execution's temporary `QueryExecution` mapping, which happens
 * for fast queries and backlogged listener queues. The listener must not discard the start
 * callback; it buffers it in a pending context and upgrades it once a query execution can be
 * resolved, emitting `START` before the terminal event with the same run id.
 */
class OpenLineageSparkListenerPendingSqlExecutionTest {

  private static final String SQL_EXECUTION_ID_KEY = "spark.sql.execution.id";

  private final SparkSession sparkSession = mock(SparkSession.class);
  private final SparkContext sparkContext = mock(SparkContext.class);
  private final EventEmitter emitter = mock(EventEmitter.class);
  private final QueryExecution queryExecution = mock(QueryExecution.class);
  private final SparkPlan sparkPlan = mock(SparkPlan.class);
  private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

  private ControllableContextFactory contextFactory;
  private OpenLineageSparkListener listener;

  @BeforeEach
  void setup() {
    when(sparkSession.sparkContext()).thenReturn(sparkContext);
    when(sparkContext.appName()).thenReturn("appName");
    when(sparkContext.applicationId()).thenReturn("application_123_234");
    when(sparkContext.getConf()).thenReturn(new SparkConf());
    when(sparkPlan.sparkContext()).thenReturn(sparkContext);
    when(sparkPlan.nodeName()).thenReturn("execute");

    when(emitter.getJobNamespace()).thenReturn("ns_name");
    when(emitter.getParentJobName()).thenReturn(Optional.of("parent_name"));
    when(emitter.getParentJobNamespace()).thenReturn(Optional.of("parent_namespace"));
    when(emitter.getParentRunId())
        .thenReturn(Optional.of(UUID.fromString("8d99e33e-2a1c-4254-9600-18f23435fc3b")));
    when(emitter.getApplicationRunId())
        .thenReturn(UUID.fromString("8d99e33e-bbbb-cccc-dddd-18f2343aaaaa"));
    when(emitter.getApplicationJobName()).thenReturn("test_rdd");

    when(queryExecution.optimizedPlan()).thenReturn(insertIntoRelationPlan());
    when(queryExecution.executedPlan()).thenReturn(sparkPlan);

    contextFactory = new ControllableContextFactory();
    listener = new OpenLineageSparkListener(new SparkConf());
    listener.skipInitializationForTests(contextFactory);
  }

  @AfterEach
  void teardown() {
    OpenLineageSparkListener.resetDefaultFactoryForTests();
    JobMetricsHolder.getInstance().cleanUpAll();
  }

  /**
   * When the query execution is available at SQL start, the registered context handles the whole
   * lifecycle and no pending context is involved.
   */
  @Test
  void testQueryExecutionAvailableAtSqlStartUsesRegisteredContext() {
    ExecutionContext executionContext = mock(ExecutionContext.class);
    contextFactory.startLookup = Optional.of(executionContext);
    SparkListenerSQLExecutionStart startEvent = sqlStartEvent(63L, 1_000L);
    SparkListenerSQLExecutionEnd endEvent = sqlEndEvent(63L, 9_000L);

    listener.onOtherEvent(startEvent);
    listener.onOtherEvent(endEvent);

    InOrder inOrder = inOrder(executionContext);
    inOrder.verify(executionContext).start(startEvent);
    inOrder.verify(executionContext).end(endEvent);

    assertThat(contextFactory.jobCallbackRunIds).isEmpty();
    assertThat(contextFactory.endEventRunIds).isEmpty();
    assertThat(meterRegistry.get(OpenLineageSparkListener.SQL_REGISTRY_GAUGE).gauge().value())
        .isZero();
  }

  /**
   * The query execution is unavailable at SQL start but becomes available again before an
   * associated job callback. The buffered start event is emitted first, with its original event
   * time, and all events of the execution share one run id.
   */
  @Test
  @SuppressWarnings("PMD.JUnitTestContainsTooManyAsserts")
  void testQueryExecutionMissedAtSqlStartRecoveredAtJobCallback() {
    contextFactory.buildContextAtJobCallback = true;
    SparkListenerSQLExecutionStart startEvent = sqlStartEvent(63L, 1_000L);
    SparkListenerJobStart jobStart = sqlJobStartEvent(61, 63L, 1_300L);
    SparkListenerJobEnd jobEnd = sqlJobEndEvent(61, 1_500L);
    SparkListenerSQLExecutionEnd endEvent = sqlEndEvent(63L, 9_000L);

    try (MockedStatic<EventFilterUtils> utils = mockStatic(EventFilterUtils.class)) {
      utils.when(() -> EventFilterUtils.isDisabled(any(), any())).thenReturn(false);

      listener.onOtherEvent(startEvent);
      listener.onJobStart(jobStart);
      listener.onJobEnd(jobEnd);
      listener.onOtherEvent(endEvent);
    }

    List<RunEvent> events = emittedEvents(4);
    assertThat(events.stream().map(RunEvent::getEventType))
        .containsExactly(
            OpenLineage.RunEvent.EventType.START,
            OpenLineage.RunEvent.EventType.RUNNING,
            OpenLineage.RunEvent.EventType.RUNNING,
            OpenLineage.RunEvent.EventType.COMPLETE);
    // the start event keeps the original Spark event time, although it is emitted late
    assertThat(events.get(0).getEventTime()).isEqualTo(toZonedTime(1_000L));
    // all events share the run id generated for the buffered start callback
    assertThat(events.stream().map(event -> event.getRun().getRunId()).distinct())
        .containsExactly(contextFactory.jobCallbackRunIds.get(63L).get(0));
    assertThat(contextFactory.endEventRunIds).isEmpty();
    assertThat(events.stream().map(event -> event.getJob().getName()).distinct()).hasSize(1);
    assertThat(meterRegistry.get(OpenLineageSparkListener.SQL_REGISTRY_GAUGE).gauge().value())
        .isZero();
  }

  /**
   * The query execution is available only through the SQL end event. The buffered start callback is
   * recovered there: `START` is emitted before the terminal event, both share one run id and the
   * start event keeps its original event time. This is the reproduction of the race where a fast
   * execution removes its `QueryExecution` mapping before the listener processes the queued start
   * event; the buggy behavior emitted a single terminal event.
   */
  @Test
  @SuppressWarnings("PMD.JUnitTestContainsTooManyAsserts")
  void testQueryExecutionAvailableOnlyThroughSqlEndEventRecoversStartEvent() {
    contextFactory.buildContextAtEndEvent = true;
    SparkListenerSQLExecutionStart startEvent = sqlStartEvent(63L, 1_000L);
    SparkListenerSQLExecutionEnd endEvent = sqlEndEvent(63L, 9_000L);

    try (MockedStatic<EventFilterUtils> utils = mockStatic(EventFilterUtils.class)) {
      utils.when(() -> EventFilterUtils.isDisabled(any(), any())).thenReturn(false);

      listener.onOtherEvent(startEvent);
      listener.onOtherEvent(endEvent);
    }

    List<RunEvent> events = emittedEvents(2);
    assertThat(events.get(0).getEventType()).isEqualTo(OpenLineage.RunEvent.EventType.START);
    assertThat(events.get(1).getEventType()).isEqualTo(OpenLineage.RunEvent.EventType.COMPLETE);
    assertThat(events.get(0).getEventTime()).isEqualTo(toZonedTime(1_000L));
    assertThat(events.get(1).getEventTime()).isEqualTo(toZonedTime(9_000L));
    // START and the terminal event share the run id generated when the start callback was buffered
    assertThat(events.get(0).getRun().getRunId())
        .isEqualTo(events.get(1).getRun().getRunId())
        .isEqualTo(contextFactory.endEventRunIds.get(63L).get(0));
    assertThat(events.get(0).getJob().getName()).isEqualTo(events.get(1).getJob().getName());
    assertThat(events.get(0).getOutputs()).isNotEmpty();
    assertThat(meterRegistry.get(OpenLineageSparkListener.SQL_REGISTRY_GAUGE).gauge().value())
        .isZero();
    assertThat(
            meterRegistry
                .get(OpenLineageSparkListener.METRICS_EXECUTION_GROUPS_GAUGE)
                .gauge()
                .value())
        .isZero();
  }

  /**
   * Neither the job callbacks nor the SQL end event provide a query execution: no event is emitted,
   * the start callback is not silently re-processed, and the pending state is cleaned.
   */
  @Test
  @SuppressWarnings("PMD.JUnitTestContainsTooManyAsserts")
  void testQueryExecutionUnavailableForEntireLifecycleEmitsNothing() {
    SparkListenerSQLExecutionStart startEvent = sqlStartEvent(63L, 1_000L);
    SparkListenerJobStart jobStart = sqlJobStartEvent(61, 63L, 1_300L);
    SparkListenerJobEnd jobEnd = sqlJobEndEvent(61, 1_500L);
    SparkListenerSQLExecutionEnd endEvent = sqlEndEvent(63L, 9_000L);

    listener.onOtherEvent(startEvent);
    listener.onJobStart(jobStart);
    listener.onJobEnd(jobEnd);
    listener.onOtherEvent(endEvent);

    verify(emitter, never()).emit(any());
    assertThat(contextFactory.jobCallbackRunIds.get(63L)).hasSize(2);
    assertThat(contextFactory.endEventRunIds.get(63L)).hasSize(1);
    assertThat(meterRegistry.get(OpenLineageSparkListener.SQL_REGISTRY_GAUGE).gauge().value())
        .isZero();
    assertThat(
            meterRegistry
                .get(OpenLineageSparkListener.METRICS_EXECUTION_GROUPS_GAUGE)
                .gauge()
                .value())
        .isZero();
  }

  /**
   * Several executions miss their query execution at start. One recovers on a job callback, the
   * other only on the SQL end event; callbacks are never mixed between the executions.
   */
  @Test
  void testInterleavedPendingExecutionsAreRecoveredIndependently() {
    ExecutionContext recoveredAtJobContext = mock(ExecutionContext.class);
    ExecutionContext recoveredAtEndContext = mock(ExecutionContext.class);
    contextFactory.jobCallbackLookup = Optional.of(recoveredAtJobContext);
    contextFactory.jobCallbackLookupExecutionId = 63L;
    contextFactory.endEventLookup = Optional.of(recoveredAtEndContext);

    SparkListenerSQLExecutionStart startOne = sqlStartEvent(63L, 1_000L);
    SparkListenerSQLExecutionStart startTwo = sqlStartEvent(64L, 2_000L);
    SparkListenerJobStart jobStartOne = sqlJobStartEvent(61, 63L, 1_300L);
    SparkListenerJobStart jobStartTwo = sqlJobStartEvent(62, 64L, 1_400L);
    SparkListenerJobEnd jobEndOne = sqlJobEndEvent(61, 1_500L);
    SparkListenerJobEnd jobEndTwo = sqlJobEndEvent(62, 1_600L);
    SparkListenerSQLExecutionEnd endOne = sqlEndEvent(63L, 9_000L);
    SparkListenerSQLExecutionEnd endTwo = sqlEndEvent(64L, 9_500L);

    listener.onOtherEvent(startOne);
    listener.onOtherEvent(startTwo);
    listener.onJobStart(jobStartOne);
    listener.onJobStart(jobStartTwo);
    listener.onJobEnd(jobEndOne);
    listener.onJobEnd(jobEndTwo);
    listener.onOtherEvent(endOne);
    listener.onOtherEvent(endTwo);

    // execution 63 recovered at the job callback: buffered start, then the job and end callbacks
    InOrder recoveredAtJobOrder = inOrder(recoveredAtJobContext);
    recoveredAtJobOrder.verify(recoveredAtJobContext).start(startOne);
    recoveredAtJobOrder.verify(recoveredAtJobContext).start(jobStartOne);
    recoveredAtJobOrder.verify(recoveredAtJobContext).end(jobEndOne);
    recoveredAtJobOrder.verify(recoveredAtJobContext).end(endOne);

    // execution 64 recovered at the SQL end event: buffered start first, then the end callback
    InOrder recoveredAtEndOrder = inOrder(recoveredAtEndContext);
    recoveredAtEndOrder.verify(recoveredAtEndContext).start(startTwo);
    recoveredAtEndOrder.verify(recoveredAtEndContext).end(endTwo);

    // job callbacks of execution 64 were observed while it was still pending, but never replayed
    verify(recoveredAtEndContext, never()).start(any(SparkListenerJobStart.class));
    verify(recoveredAtEndContext, never()).end(any(SparkListenerJobEnd.class));

    // execution 63 was upgraded on its job callback, execution 64 retried twice with the run id
    // of its pending record and was then upgraded with the same run id on its SQL end event
    assertThat(contextFactory.jobCallbackRunIds.get(63L)).hasSize(1);
    assertThat(contextFactory.jobCallbackRunIds.get(64L)).hasSize(2);
    assertThat(contextFactory.endEventRunIds.keySet()).containsExactly(64L);
    assertThat(contextFactory.jobCallbackRunIds.get(64L))
        .containsOnly(contextFactory.endEventRunIds.get(64L).get(0));
    // both executions were buffered in their own pending record, so their run ids differ
    assertThat(contextFactory.jobCallbackRunIds.get(63L).get(0))
        .isNotEqualTo(contextFactory.endEventRunIds.get(64L).get(0));
    assertThat(meterRegistry.get(OpenLineageSparkListener.SQL_REGISTRY_GAUGE).gauge().value())
        .isZero();
  }

  /** Application shutdown discards pending execution records without emitting events for them. */
  @Test
  void testApplicationShutdownWithPendingExecutionRecords() {
    ExecutionContext applicationContext = mock(ExecutionContext.class);
    contextFactory.applicationContext = applicationContext;
    SparkListenerSQLExecutionStart startEvent = sqlStartEvent(63L, 1_000L);
    SparkListenerApplicationEnd applicationEnd = mock(SparkListenerApplicationEnd.class);

    listener.onOtherEvent(startEvent);
    assertThat(meterRegistry.get(OpenLineageSparkListener.SQL_REGISTRY_GAUGE).gauge().value())
        .isEqualTo(1);

    listener.onApplicationEnd(applicationEnd);

    verify(applicationContext).end(applicationEnd);
    verify(emitter, never()).emit(any());
    assertThat(contextFactory.jobCallbackRunIds).isEmpty();
    assertThat(contextFactory.endEventRunIds).isEmpty();
    assertThat(meterRegistry.get(OpenLineageSparkListener.SQL_REGISTRY_GAUGE).gauge().value())
        .isZero();
  }

  /** A filtered execution recovers into a context that skips its events without leaking state. */
  @Test
  void testFilteredExecutionDoesNotLeakPendingState() {
    contextFactory.buildContextAtEndEvent = true;
    SparkListenerSQLExecutionStart startEvent = sqlStartEvent(63L, 1_000L);
    SparkListenerSQLExecutionEnd endEvent = sqlEndEvent(63L, 9_000L);

    try (MockedStatic<EventFilterUtils> utils = mockStatic(EventFilterUtils.class)) {
      utils.when(() -> EventFilterUtils.isDisabled(any(), any())).thenReturn(true);

      listener.onOtherEvent(startEvent);
      listener.onOtherEvent(endEvent);
    }

    verify(emitter, never()).emit(any());
    assertThat(contextFactory.endEventRunIds.get(63L)).hasSize(1);
    assertThat(meterRegistry.get(OpenLineageSparkListener.SQL_REGISTRY_GAUGE).gauge().value())
        .isZero();
    assertThat(
            meterRegistry
                .get(OpenLineageSparkListener.METRICS_EXECUTION_GROUPS_GAUGE)
                .gauge()
                .value())
        .isZero();
  }

  private List<RunEvent> emittedEvents(int expectedCount) {
    ArgumentCaptor<RunEvent> lineageEvent = ArgumentCaptor.forClass(RunEvent.class);
    verify(emitter, times(expectedCount)).emit(lineageEvent.capture());
    return lineageEvent.getAllValues();
  }

  private SparkListenerSQLExecutionStart sqlStartEvent(long executionId, long time) {
    SparkListenerSQLExecutionStart startEvent = mock(SparkListenerSQLExecutionStart.class);
    when(startEvent.executionId()).thenReturn(executionId);
    when(startEvent.time()).thenReturn(time);
    when(startEvent.sparkPlanInfo())
        .thenReturn(
            new SparkPlanInfo(
                "name",
                "string",
                ScalaConversionUtils.asScalaSeqEmpty(),
                ScalaConversionUtils.asScalaMapEmpty(),
                ScalaConversionUtils.asScalaSeqEmpty()));
    return startEvent;
  }

  private SparkListenerSQLExecutionEnd sqlEndEvent(long executionId, long time) {
    SparkListenerSQLExecutionEnd endEvent = mock(SparkListenerSQLExecutionEnd.class);
    when(endEvent.executionId()).thenReturn(executionId);
    when(endEvent.time()).thenReturn(time);
    return endEvent;
  }

  private SparkListenerJobStart sqlJobStartEvent(int jobId, long executionId, long time) {
    SparkListenerJobStart jobStart = mock(SparkListenerJobStart.class);
    when(jobStart.jobId()).thenReturn(jobId);
    when(jobStart.time()).thenReturn(time);
    when(jobStart.stageIds()).thenReturn(ScalaConversionUtils.asScalaSeqEmpty());
    Properties properties = new Properties();
    properties.setProperty(SQL_EXECUTION_ID_KEY, String.valueOf(executionId));
    when(jobStart.properties()).thenReturn(properties);
    return jobStart;
  }

  private SparkListenerJobEnd sqlJobEndEvent(int jobId, long time) {
    SparkListenerJobEnd jobEnd = mock(SparkListenerJobEnd.class);
    when(jobEnd.jobId()).thenReturn(jobId);
    when(jobEnd.time()).thenReturn(time);
    return jobEnd;
  }

  private InsertIntoHadoopFsRelationCommand insertIntoRelationPlan() {
    LogicalPlan query = UnresolvedRelation$.MODULE$.apply(TableIdentifier.apply("tableName"));
    return new InsertIntoHadoopFsRelationCommand(
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
        ScalaConversionUtils.<String>asScalaSeqEmpty());
  }

  /**
   * Builds a fully functional context that emits real run events for the given run id, which is the
   * run id buffered in the pending context.
   */
  private ExecutionContext contextForRunId(long executionId, UUID runId) {
    SparkOpenLineageConfig config = new SparkOpenLineageConfig();
    OpenLineageContext olContext =
        OpenLineageContext.builder()
            .sparkSession(sparkSession)
            .sparkContext(sparkContext)
            .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
            .queryExecution(queryExecution)
            .runUuid(runId)
            .meterRegistry(meterRegistry)
            .openLineageConfig(config)
            .sparkExtensionVisitorWrapper(new SparkOpenLineageExtensionVisitorWrapper(config))
            .build();
    olContext
        .getOutputDatasetQueryPlanVisitors()
        .add(new InsertIntoHadoopFsRelationVisitor(olContext));
    return new StaticExecutionContextFactory(emitter, meterRegistry, config)
        .createSparkSQLExecutionContext(executionId, emitter, queryExecution, olContext);
  }

  /**
   * {@link ContextFactory} controlled by the test. The listener misses the query execution at SQL
   * start unless {@code startLookup} is set; job callbacks and the SQL end event resolve a context
   * only when the test enables it. All invocations of the run-id overloads are recorded.
   */
  private class ControllableContextFactory extends ContextFactory {

    Optional<ExecutionContext> startLookup = Optional.empty();
    Optional<ExecutionContext> jobCallbackLookup = Optional.empty();
    Optional<ExecutionContext> endEventLookup = Optional.empty();
    long jobCallbackLookupExecutionId = Long.MIN_VALUE;
    boolean buildContextAtJobCallback = false;
    boolean buildContextAtEndEvent = false;
    ExecutionContext applicationContext = mock(ExecutionContext.class);

    final Map<Long, List<UUID>> jobCallbackRunIds = new LinkedHashMap<>();
    final Map<Long, List<UUID>> endEventRunIds = new LinkedHashMap<>();

    private void record(Map<Long, List<UUID>> calls, long executionId, UUID runId) {
      calls.computeIfAbsent(executionId, id -> new ArrayList<>()).add(runId);
    }

    ControllableContextFactory() {
      super(emitter, meterRegistry, new SparkOpenLineageConfig());
    }

    @Override
    public Optional<ExecutionContext> createSparkSQLExecutionContext(long executionId) {
      return startLookup;
    }

    @Override
    public Optional<ExecutionContext> createSparkSQLExecutionContext(long executionId, UUID runId) {
      record(jobCallbackRunIds, executionId, runId);
      if (buildContextAtJobCallback) {
        return Optional.of(contextForRunId(executionId, runId));
      }
      if (executionId == jobCallbackLookupExecutionId) {
        return jobCallbackLookup;
      }
      return Optional.empty();
    }

    @Override
    public Optional<ExecutionContext> createSparkSQLExecutionContext(
        SparkListenerSQLExecutionEnd event, UUID runId) {
      record(endEventRunIds, event.executionId(), runId);
      if (buildContextAtEndEvent) {
        return Optional.of(contextForRunId(event.executionId(), runId));
      }
      return endEventLookup;
    }

    @Override
    public ExecutionContext createSparkApplicationExecutionContext(SparkContext context) {
      return applicationContext;
    }
  }
}
