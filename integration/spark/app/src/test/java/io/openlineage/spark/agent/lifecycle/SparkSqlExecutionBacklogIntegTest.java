/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.spark.agent.EventEmitter;
import io.openlineage.spark.agent.OpenLineageSparkListener;
import io.openlineage.spark.agent.Spark4CompatUtils;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

/**
 * Reproduces the asynchronous mapping-removal order of {@code SQLExecution.withNewExecutionId}:
 * Spark removes the execution's temporary {@code QueryExecution} mapping before a backlogged
 * listener queue processes the queued start event. A listener registered before {@link
 * OpenLineageSparkListener} blocks the shared listener queue with a latch until the queries have
 * finished, so the OpenLineage listener processes every SQL start event after its mapping was
 * removed. No sleep-based assertion is used; processing is unblocked and observed with latches
 * only.
 */
@Tag("integration-test")
class SparkSqlExecutionBacklogIntegTest {

  private static final int QUERY_COUNT = 6;
  private static final UUID APPLICATION_RUN_ID =
      UUID.fromString("8d99e33e-bbbb-cccc-dddd-18f2343aaaaa");

  private static final EventEmitter emitter = mock(EventEmitter.class);

  @AfterAll
  static void resetListener() {
    OpenLineageSparkListener.resetDefaultFactoryForTests();
    Spark4CompatUtils.cleanupAnyExistingSession();
  }

  @Test
  @SuppressWarnings("PMD.JUnitTestContainsTooManyAsserts")
  void fastSqlExecutionsEmitStartAndTerminalEventAfterListenerBacklog(@TempDir Path outputRoot)
      throws Exception {
    // a reset test emitter makes the test repeatable within one JVM, as the CI retry mechanism does
    reset(emitter);
    when(emitter.getJobNamespace()).thenReturn("ns_name");
    when(emitter.getParentJobName()).thenReturn(Optional.of("parent_name"));
    when(emitter.getParentJobNamespace()).thenReturn(Optional.of("parent_namespace"));
    when(emitter.getParentRunId())
        .thenReturn(Optional.of(UUID.fromString("8d99e33e-2a1c-4254-9600-18f23435fc3b")));
    when(emitter.getApplicationRunId()).thenReturn(APPLICATION_RUN_ID);
    when(emitter.getApplicationJobName()).thenReturn("test_rdd");

    OpenLineageSparkListener.overrideDefaultFactoryForTests(
        new ContextFactory(emitter, new SimpleMeterRegistry(), new SparkOpenLineageConfig()));
    Spark4CompatUtils.cleanupAnyExistingSession();

    SparkSession spark = sparkSession();

    // Many short SQL commands run while the listener queue is blocked; Spark removes their
    // QueryExecution mapping before the OpenLineage listener processes the queued start events.
    for (int query = 0; query < QUERY_COUNT; query++) {
      spark
          .range(0, 5, 1, 1)
          .write()
          .mode("overwrite")
          .parquet(outputRoot.resolve("output_" + query).toString());
    }

    // A listener registered after OpenLineageSparkListener counts processed SQL end events; it
    // runs after the OpenLineage listener for every event, so the latch is released only when all
    // backlog has been processed by the OpenLineage listener.
    CountDownLatch executionEndsProcessed = new CountDownLatch(1);
    spark
        .sparkContext()
        .addSparkListener(new SqlExecutionEndAwaiter(QUERY_COUNT, executionEndsProcessed));

    SqlExecutionEventBlocker.unblock();

    assertThat(executionEndsProcessed.await(2, TimeUnit.MINUTES))
        .as("all %d SQL execution end events were processed", QUERY_COUNT)
        .isTrue();

    ArgumentCaptor<RunEvent> lineageEvent = ArgumentCaptor.forClass(RunEvent.class);
    verify(emitter, atLeastOnce()).emit(lineageEvent.capture());

    Map<UUID, List<RunEvent>> eventsByRunId =
        lineageEvent.getAllValues().stream()
            .filter(event -> !APPLICATION_RUN_ID.equals(event.getRun().getRunId()))
            .collect(
                Collectors.groupingBy(
                    event -> event.getRun().getRunId(),
                    Collectors.mapping(event -> event, Collectors.toList())));

    // every SQL execution emits a start event; no run is terminal-only
    assertThat(eventsByRunId).hasSize(QUERY_COUNT);
    assertThat(
            eventsByRunId.values().stream()
                .filter(events -> events.get(0).getEventType() != RunEvent.EventType.START))
        .isEmpty();

    // each short execution emitted START before the terminal event, with one run id, a stable job
    // name, and the original start event time
    for (int query = 0; query < QUERY_COUNT; query++) {
      List<RunEvent> events = eventsOfWrite(lineageEvent.getAllValues(), outputRoot, query);
      assertThat(events).hasSize(2);
      assertThat(events.get(0).getEventType()).isEqualTo(RunEvent.EventType.START);
      assertThat(events.get(1).getEventType()).isEqualTo(RunEvent.EventType.COMPLETE);
      assertThat(events.get(1).getRun().getRunId()).isEqualTo(events.get(0).getRun().getRunId());
      assertThat(events.get(1).getJob().getName()).isEqualTo(events.get(0).getJob().getName());
      assertThat(events.get(0).getEventTime()).isBeforeOrEqualTo(events.get(1).getEventTime());
    }
  }

  private List<RunEvent> eventsOfWrite(List<RunEvent> events, Path outputRoot, int query) {
    String outputName = outputRoot.resolve("output_" + query).toString();
    return events.stream()
        .filter(
            event ->
                event.getOutputs().stream()
                    .anyMatch(
                        output ->
                            output.getNamespace().contains("file")
                                && output.getName().equals(outputName)))
        .collect(Collectors.toList());
  }

  private SparkSession sparkSession() {
    String testName = "fastSqlExecutionsEmitStartAndTerminalEvent";
    String warehouseDir =
        Paths.get(System.getProperty("spark.sql.warehouse.dir"))
            .toAbsolutePath()
            .resolve(testName)
            .resolve(String.valueOf(Instant.now().getEpochSecond()))
            .toString();
    String derbyHome =
        Paths.get(System.getProperty("derby.system.home.base"))
            .toAbsolutePath()
            .resolve(testName)
            .resolve(String.valueOf(Instant.now().getEpochSecond()))
            .toString();
    System.setProperty("derby.system.home", derbyHome);

    return SparkSession.builder()
        .master("local[2]")
        .appName(testName)
        // the blocker must be registered before OpenLineageSparkListener so it delays the
        // listener queue
        .config(
            "spark.extraListeners",
            SqlExecutionEventBlocker.class.getName()
                + ","
                + OpenLineageSparkListener.class.getName())
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", 1)
        .config("spark.sql.warehouse.dir", warehouseDir)
        .config("spark.ui.enabled", false)
        .getOrCreate();
  }

  /**
   * Blocks SQL execution events on the shared listener queue until {@link #unblock()} is called, so
   * every listener registered after it - including {@link OpenLineageSparkListener} - processes the
   * SQL execution events only after the SQL commands have finished and Spark removed their
   * `QueryExecution` mappings. The latch is single use; the blocker is not reusable within one JVM
   * run.
   */
  public static class SqlExecutionEventBlocker extends SparkListener {

    private static final CountDownLatch RELEASE = new CountDownLatch(1);

    static void unblock() {
      RELEASE.countDown();
    }

    @Override
    public void onOtherEvent(SparkListenerEvent event) {
      if (event instanceof SparkListenerSQLExecutionStart
          || event instanceof SparkListenerSQLExecutionEnd) {
        try {
          RELEASE.await(2, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  /** Counts SQL execution end events that were processed by all listeners registered before it. */
  static class SqlExecutionEndAwaiter extends SparkListener {

    private final int expectedExecutionEnds;
    private final CountDownLatch executionEndsProcessed;
    private final AtomicInteger processedExecutionEnds = new AtomicInteger();

    SqlExecutionEndAwaiter(int expectedExecutionEnds, CountDownLatch executionEndsProcessed) {
      this.expectedExecutionEnds = expectedExecutionEnds;
      this.executionEndsProcessed = executionEndsProcessed;
    }

    @Override
    public void onOtherEvent(SparkListenerEvent event) {
      if (event instanceof SparkListenerSQLExecutionEnd
          && processedExecutionEnds.incrementAndGet() >= expectedExecutionEnds) {
        executionEndsProcessed.countDown();
      }
    }
  }
}
