/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.MockServerUtils.getEventsEmitted;
import static io.openlineage.spark.agent.SparkTestUtils.SPARK_3_OR_ABOVE;
import static io.openlineage.spark.agent.SparkTestUtils.SPARK_VERSION;
import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage.RunEvent;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.mockserver.integration.ClientAndServer;

/**
 * Integration tests for the Structured Streaming micro-batch throttle feature. These tests verify:
 *
 * <ol>
 *   <li>Throttling reduces the number of emitted lineage events for a long-running streaming query.
 *   <li>Multiple concurrent streaming queries are throttled <em>independently</em> — each query's
 *       counter starts at zero, so a second query is not penalised by the first query's batches.
 * </ol>
 */
@Slf4j
@Tag("integration-test")
class StreamingThrottleIntegrationTest {

  private static final int MOCK_SERVER_PORT = 1092;
  private static final String LOCAL_IP = "127.0.0.1";

  private ClientAndServer mockServer;
  private SparkSession spark;

  @BeforeEach
  @SneakyThrows
  void beforeEach() {
    Spark4CompatUtils.cleanupAnyExistingSession();
    mockServer = MockServerUtils.createAndConfigureMockServer(MOCK_SERVER_PORT);
  }

  @AfterEach
  @SneakyThrows
  void afterEach() {
    if (spark != null && !spark.sparkContext().isStopped()) {
      spark.stop();
    }
    MockServerUtils.stopMockServer(mockServer);
    Spark4CompatUtils.cleanupAnyExistingSession();
  }

  /**
   * Verifies end-to-end that the count-based throttle suppresses most micro-batch events. With
   * N=1000 only batch 0 (counter=0, 0%1000==0) emits. After letting 5+ batches run, exactly one
   * unique streaming run should be visible in the lineage output.
   */
  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = SPARK_3_OR_ABOVE)
  void throttleN1000EmitsOnlyFirstBatch() throws Exception {
    spark = buildSparkSession("throttleN1000test", 1000);

    // Rate source produces continuous data; ProcessingTime batches it at fixed intervals.
    StreamingQuery q =
        spark
            .readStream()
            .format("rate")
            .option("rowsPerSecond", "100")
            .load()
            .writeStream()
            .format("console")
            .trigger(Trigger.ProcessingTime(Duration.ofMillis(100).toMillis()))
            .start();

    // Wait until at least 5 batches have completed (batchId is 0-based).
    Awaitility.await()
        .pollInterval(Duration.ofMillis(50))
        .atMost(Duration.ofSeconds(30))
        .until(() -> q.lastProgress() != null && q.lastProgress().batchId() >= 4);

    q.stop();
    spark.stop();

    List<RunEvent> streamingEvents = streamingEventsFrom(getEventsEmitted(mockServer));

    // Each non-throttled batch produces events with a unique run ID.
    // With N=1000, only batch 0 should have emitted → 1 distinct run ID.
    long distinctRuns = streamingEvents.stream().map(e -> e.getRun().getRunId()).distinct().count();

    assertThat(distinctRuns)
        .as(
            "with N=1000 throttle, only the first micro-batch should produce lineage events"
                + " (got %d distinct runs across %d streaming events)",
            distinctRuns, streamingEvents.size())
        .isEqualTo(1);
  }

  /**
   * Verifies that each streaming query maintains its own independent throttle counter.
   *
   * <p>With N=1000, only a job's very first batch (counter=0, 0%1000==0) ever emits. When two
   * different streaming queries run <em>sequentially</em>, query A advances the counter for
   * <em>its</em> key, while query B starts at counter=0 for <em>its</em> key. Both must emit
   * exactly one lineage event.
   *
   * <p>With the old single-global-counter design, query A's call would advance the shared counter
   * to 1; query B's first call would then see counter=1, and 1%1000≠0, so B would be silently
   * suppressed despite being a completely different streaming job.
   */
  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = SPARK_3_OR_ABOVE)
  void twoQueriesRunSequentiallyBothEmitIndependently() throws Exception {
    spark = buildSparkSession("throttleIndependenceTest", 1000);

    // Query A — writes to a unique temp path so its OL job name includes the path as a suffix,
    // making it distinct from query B's job name and thus an independent throttle key.
    // Runs until at least 2 batches complete, then stops so A's per-job counter advances beyond 0.
    String pathA = "/tmp/ol-stream-a-" + UUID.randomUUID();
    String pathB = "/tmp/ol-stream-b-" + UUID.randomUUID();
    StreamingQuery qA =
        spark
            .readStream()
            .format("rate")
            .option("rowsPerSecond", "100")
            .load()
            .writeStream()
            .format("csv")
            .option("path", pathA)
            .option("checkpointLocation", pathA + "-checkpoint")
            .trigger(Trigger.ProcessingTime(Duration.ofMillis(100).toMillis()))
            .start();

    Awaitility.await()
        .pollInterval(Duration.ofMillis(50))
        .atMost(Duration.ofSeconds(30))
        .until(() -> qA.lastProgress() != null && qA.lastProgress().batchId() >= 1);

    qA.stop();
    MockServerUtils.clearRequests(mockServer);

    // Query B — different output path → different OL job name suffix → independent throttle key.
    // Its first batch must emit even though A already ran multiple batches.
    StreamingQuery qB =
        spark
            .readStream()
            .format("rate")
            .option("rowsPerSecond", "100")
            .load()
            .writeStream()
            .format("csv")
            .option("path", pathB)
            .option("checkpointLocation", pathB + "-checkpoint")
            .trigger(Trigger.ProcessingTime(Duration.ofMillis(100).toMillis()))
            .start();

    Awaitility.await()
        .pollInterval(Duration.ofMillis(50))
        .atMost(Duration.ofSeconds(30))
        .until(() -> qB.lastProgress() != null && qB.lastProgress().batchId() >= 1);

    qB.stop();
    spark.stop();

    // Check events collected after A stopped (i.e., only B's events).
    List<RunEvent> bEvents = streamingEventsFrom(getEventsEmitted(mockServer));

    assertThat(bEvents)
        .as(
            "query B must emit lineage events on its first batch (per-job throttle state);"
                + " with a shared global counter B's counter would be >0 and throttled")
        .isNotEmpty();
  }

  /**
   * Verifies that throttling applies only to streaming queries — a batch query started after a
   * throttled streaming query still emits its lineage events normally.
   */
  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = SPARK_3_OR_ABOVE)
  void batchQueryAfterStreamingIsNotThrottled() throws Exception {
    // N=1000: any streaming batch after the first is suppressed.
    spark = buildSparkSession("throttleBatchAfterStreamingTest", 1000);

    // Run several streaming batches so the throttle counter advances well past 0.
    StreamingQuery q =
        spark
            .readStream()
            .format("rate")
            .option("rowsPerSecond", "100")
            .load()
            .writeStream()
            .format("console")
            .trigger(Trigger.ProcessingTime(Duration.ofMillis(100).toMillis()))
            .start();

    Awaitility.await()
        .pollInterval(Duration.ofMillis(50))
        .atMost(Duration.ofSeconds(30))
        .until(() -> q.lastProgress() != null && q.lastProgress().batchId() >= 4);

    q.stop();

    // Now run a plain batch query — must not be throttled.
    spark
        .read()
        .format("csv")
        .schema("name STRING, date DATE, location STRING")
        .load("src/test/resources/streaming/csvinput")
        .write()
        .format("csv")
        .mode("overwrite")
        .save("/tmp/ol-batch-output-" + System.nanoTime());

    spark.stop();

    List<RunEvent> events = getEventsEmitted(mockServer);

    long batchCompletes =
        events.stream()
            .filter(
                e ->
                    e.getJob().getFacets().getJobType() != null
                        && "BATCH".equals(e.getJob().getFacets().getJobType().getProcessingType()))
            .filter(e -> RunEvent.EventType.COMPLETE.equals(e.getEventType()))
            .count();

    assertThat(batchCompletes)
        .as(
            "batch query after streaming must still emit a COMPLETE event (throttle is streaming-only)")
        .isGreaterThanOrEqualTo(1);
  }

  private SparkSession buildSparkSession(String appName, int microbatchThrottle) {
    return SparkSession.builder()
        .master("local[*]")
        .appName(appName)
        .config("spark.driver.host", LOCAL_IP)
        .config("spark.driver.bindAddress", LOCAL_IP)
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.extraListeners", OpenLineageSparkListener.class.getName())
        .config("spark.openlineage.transport.type", "http")
        .config(
            "spark.openlineage.transport.url",
            "http://localhost:" + mockServer.getPort() + "/api/v1/lineage")
        .config("spark.openlineage.namespace", "test-namespace")
        .config(
            "spark.openlineage.filter.streaming.microbatchThrottle",
            String.valueOf(microbatchThrottle))
        .config("spark.ui.enabled", "false")
        .getOrCreate();
  }

  private List<RunEvent> streamingEventsFrom(List<RunEvent> events) {
    return events.stream()
        .filter(
            e ->
                e.getJob().getFacets().getJobType() != null
                    && "STREAMING".equals(e.getJob().getFacets().getJobType().getProcessingType()))
        .collect(Collectors.toList());
  }
}
