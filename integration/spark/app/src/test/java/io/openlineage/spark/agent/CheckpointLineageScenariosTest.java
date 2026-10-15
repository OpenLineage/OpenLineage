/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static org.apache.spark.sql.functions.max;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.LogicalRDD;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.mockserver.integration.ClientAndServer;

/**
 * Scenarios illustrating how checkpoint lineage is captured and looked up, meant to be stepped
 * through with a debugger. Useful breakpoints:
 *
 * <ul>
 *   <li>{@code SparkSQLExecutionContext#start(SparkListenerSQLExecutionStart)} - checkpoint
 *       execution detection
 *   <li>{@code SparkSQLExecutionContext#mapCheckpointRdd} - RDD id to execution id mapping (eager
 *       checkpoints only)
 *   <li>{@code SparkSQLExecutionContext#captureCheckpointLineage} - lineage captured from the
 *       checkpointed plan
 *   <li>{@code LogicalRDDVisitor#apply} - input datasets looked up for a checkpointed RDD
 *   <li>{@code InputFieldsCollector#collectCheckpointInputs} - column lineage bridged for a
 *       checkpointed RDD
 * </ul>
 *
 * <p>Besides the lineage, the tests assert how Spark assigns {@code ExprId}s to the output of
 * checkpointed datasets, as that's what would allow looking up captured lineage without relying on
 * RDD ids (and thus supporting lazy checkpoints). Lineage of lazy checkpoints is only logged, as
 * it's currently not captured.
 */
@Slf4j
@EnabledIfSystemProperty(named = "spark.version", matches = "([34].*)")
class CheckpointLineageScenariosTest {

  @SuppressWarnings("PMD")
  private static final String LOCAL_IP = "127.0.0.1";

  private static final int MOCK_SERVER_PORT = 1092;

  private static ClientAndServer mockServer;
  private SparkSession spark;

  @TempDir Path tempDir;

  @BeforeAll
  @SneakyThrows
  static void beforeAll() {
    Spark4CompatUtils.cleanupAnyExistingSession();
    mockServer = MockServerUtils.createAndConfigureMockServer(MOCK_SERVER_PORT);
  }

  @AfterAll
  @SneakyThrows
  static void afterAll() {
    Spark4CompatUtils.cleanupAnyExistingSession();
    MockServerUtils.stopMockServer(mockServer);
  }

  @BeforeEach
  @SneakyThrows
  void beforeEach() {
    MockServerUtils.clearRequests(mockServer);
    spark =
        SparkSession.builder()
            .master("local[*]")
            .appName("CheckpointLineageScenariosTest")
            .config("spark.driver.host", LOCAL_IP)
            .config("spark.driver.bindAddress", LOCAL_IP)
            .config("spark.ui.enabled", false)
            .config("spark.sql.catalogImplementation", "in-memory")
            .config("spark.sql.warehouse.dir", tempDir.resolve("warehouse").toString())
            .config("spark.openlineage.transport.type", "http")
            .config(
                "spark.openlineage.transport.url",
                "http://localhost:" + mockServer.getPort() + "/api/v1/namespaces/checkpoint-test")
            .config("spark.extraListeners", OpenLineageSparkListener.class.getName())
            .config("spark.openlineage.checkpointLineage.enabled", true)
            .getOrCreate();
    spark
        .sparkContext()
        .setCheckpointDir(
            "/Users/tomasznazarewicz/projects/OpenLineage/integration/spark/app/tmp/spark-checkpoints");

    spark.sql("CREATE TABLE sc_t1 (a string, b string) USING parquet");
    spark.sql("INSERT INTO sc_t1 VALUES ('x', 'y'), ('z', 'w')");
    spark.sql("CREATE TABLE sc_t2 (a string, b string) USING parquet");
    spark.sql("INSERT INTO sc_t2 VALUES ('x', 'v')");
  }

  @AfterEach
  @SneakyThrows
  void afterEach() {
    spark.stop();
    Spark4CompatUtils.cleanupAnyExistingSession();
  }

  /**
   * {@code df2 = df1.transformation().checkpoint(true); df3 =
   * df2.transformation().checkpoint(true); df4 = df3.transformation().checkpoint(true)}. Each
   * checkpoint's plan reads the previous checkpointed RDD, which gets resolved to the lineage
   * captured for it, so each captured checkpoint already points at the source table rather than at
   * the previous checkpoint.
   */
  @Test
  void testEagerCheckpointChain() {
    Dataset<Row> df1 = spark.sql("SELECT a, b FROM sc_t1");
    Dataset<Row> df2 = df1.selectExpr("a", "upper(b) AS b").checkpoint(true);
    Dataset<Row> df3 =
        df2.selectExpr("a", "concat(b, '_3') AS b").filter("a = 'x'").checkpoint(true);
    Dataset<Row> df4 = df3.groupBy("a").agg(max("b").as("b")).checkpoint(true);

    RunEvent complete = completeEvent(write(df4, "sc_chain_out"));

    assertThat(complete.getInputs())
        .as("the whole chain should resolve to the source table")
        .anyMatch(ds -> ds.getName().contains("sc_t1"));
    assertThat(inputFields(complete, "sc_chain_out", "b"))
        .as("b of the last checkpoint should come from sc_t1.b through all three transformations")
        .anyMatch(f -> f.getName().contains("sc_t1") && "b".equals(f.getField()));
    assertThat(datasetDependencies(complete, "sc_chain_out"))
        .as("filter of df3 and group by of df4 should both be carried over")
        .anyMatch(f -> f.getName().contains("sc_t1") && "a".equals(f.getField()));
  }

  /**
   * A lazy checkpoint runs no job within its own execution - the RDD is computed and checkpointed
   * by the first downstream action reading it. The checkpointed dataset's output still has the same
   * {@code ExprId}s as the plan being checkpointed, even though no RDD id was mapped.
   */
  @Test
  void testLazyCheckpoint() {
    Dataset<Row> origin = spark.sql("SELECT a, upper(b) AS b FROM sc_t1 WHERE a = 'x'");
    Dataset<Row> checkpointed = origin.checkpoint(false);

    assertThat(outputExprIds(checkpointed))
        .as("checkpointed dataset should keep the ExprIds of the checkpointed plan's output")
        .isEqualTo(outputExprIds(origin));
    assertThat(checkpointedRdd(checkpointed).isCheckpointed())
        .as("lazy checkpoint should not be materialized until read")
        .isFalse();

    List<RunEvent> firstRead = write(checkpointed, "sc_lazy_first");
    assertThat(checkpointedRdd(checkpointed).isCheckpointed())
        .as("first downstream read should materialize the checkpoint")
        .isTrue();

    List<RunEvent> secondRead = write(checkpointed, "sc_lazy_second");

    log.info("Lazy checkpoint, first read (START may differ from COMPLETE):");
    firstRead.forEach(CheckpointLineageScenariosTest::logEvent);
    log.info("Lazy checkpoint, second read (after materialization):");
    secondRead.forEach(CheckpointLineageScenariosTest::logEvent);
  }

  /**
   * The SQL execution of a checkpoint is described with the call site of the {@code checkpoint()}
   * call, the same as the checkpointed RDD's creation site - which is still available once the
   * checkpointed RDD is read downstream, so (output {@code ExprId}s, description) could be used as
   * a key to look up captured lineage.
   */
  @Test
  void testCheckpointDescriptionMatchesRddCreationSite() {
    List<String> descriptions = checkpointDescriptions();

    Dataset<Row> lazy = spark.sql("SELECT a, b FROM sc_t1").checkpoint(false);
    Dataset<Row> eager = spark.sql("SELECT a, b FROM sc_t1").localCheckpoint(true);

    String lazySite = checkpointedRdd(lazy).getCreationSite();
    String eagerSite = checkpointedRdd(eager).getCreationSite();
    log.info("Lazy creation site: {}, eager creation site: {}", lazySite, eagerSite);

    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> assertThat(descriptions).contains(lazySite, eagerSite));
    assertThat(lazySite).isNotEqualTo(eagerSite);
  }

  /**
   * Checkpoints created on the same line - in a loop, or through a helper method - share both the
   * description and, if they don't change the output attributes, the output {@code ExprId}s, while
   * their lineage may differ (here: the joined input).
   */
  @Test
  void testSameLineCheckpointsShareDescriptionAndExprIds() {
    Dataset<Row> df1 = spark.sql("SELECT a, b FROM sc_t1");

    List<Dataset<Row>> inLoop = new ArrayList<>();
    for (String table : Arrays.asList("sc_t1", "sc_t2")) {
      Dataset<Row> other = spark.sql("SELECT a AS other_a FROM " + table);
      inLoop.add(
          df1.join(other, df1.col("a").equalTo(other.col("other_a")))
              .select(df1.col("a"), df1.col("b"))
              .checkpoint(false));
    }
    Dataset<Row> viaHelperOnA = checkpointLazily(df1.filter("a = 'x'"));
    Dataset<Row> viaHelperOnB = checkpointLazily(df1.filter("b = 'y'"));

    assertThat(checkpointedRdd(inLoop.get(0)).getCreationSite())
        .as("checkpoints created in a loop share the description")
        .isEqualTo(checkpointedRdd(inLoop.get(1)).getCreationSite());
    assertThat(outputExprIds(inLoop.get(0))).isEqualTo(outputExprIds(inLoop.get(1)));

    assertThat(checkpointedRdd(viaHelperOnA).getCreationSite())
        .as("checkpoints created through a helper method share the description")
        .isEqualTo(checkpointedRdd(viaHelperOnB).getCreationSite());
    assertThat(outputExprIds(viaHelperOnA)).isEqualTo(outputExprIds(viaHelperOnB));
  }

  private static Dataset<Row> checkpointLazily(Dataset<Row> dataset) {
    return dataset.checkpoint(false);
  }

  /** Collects descriptions of checkpoint SQL executions as reported to listeners. */
  private List<String> checkpointDescriptions() {
    List<String> descriptions = new CopyOnWriteArrayList<>();
    spark
        .sparkContext()
        .addSparkListener(
            new SparkListener() {
              @Override
              public void onOtherEvent(SparkListenerEvent event) {
                if (event instanceof SparkListenerSQLExecutionStart) {
                  descriptions.add(((SparkListenerSQLExecutionStart) event).description());
                }
              }
            });
    return descriptions;
  }

  /**
   * A lazy local checkpoint is found among the persisted RDDs, so its lineage is available on every
   * read - both before and after the first read materializes it.
   */
  @Test
  void testLazyLocalCheckpoint() {
    Dataset<Row> checkpointed =
        spark.sql("SELECT a, upper(b) AS b FROM sc_t1 WHERE a = 'x'").localCheckpoint(false);

    List<RunEvent> firstRead = write(checkpointed, "sc_lazy_local_first");
    List<RunEvent> secondRead = write(checkpointed, "sc_lazy_local_second");

    assertThat(firstRead)
        .as("all events of the first read should have the source table as an input")
        .allMatch(e -> e.getInputs().stream().anyMatch(ds -> ds.getName().contains("sc_t1")));
    assertThat(inputFields(completeEvent(firstRead), "sc_lazy_local_first", "b"))
        .anyMatch(f -> f.getName().contains("sc_t1") && "b".equals(f.getField()));
    assertThat(inputFields(completeEvent(secondRead), "sc_lazy_local_second", "b"))
        .anyMatch(f -> f.getName().contains("sc_t1") && "b".equals(f.getField()));
  }

  /**
   * Lazy local checkpoints created on the same line share the call site the checkpointed RDD is
   * looked up by - they're told apart by the order of creation.
   */
  @Test
  void testLazyLocalCheckpointsInLoop() {
    Dataset<Row> df1 = spark.sql("SELECT a, b FROM sc_t1");

    List<Dataset<Row>> checkpointed = new ArrayList<>();
    for (String table : Arrays.asList("sc_t1", "sc_t2")) {
      Dataset<Row> other = spark.sql("SELECT a AS other_a FROM " + table);
      checkpointed.add(
          df1.join(other, df1.col("a").equalTo(other.col("other_a")))
              .select(df1.col("a"), df1.col("b"))
              .localCheckpoint(false));
    }

    RunEvent joinedWithT1 = completeEvent(write(checkpointed.get(0), "sc_loop_joined_t1"));
    RunEvent joinedWithT2 = completeEvent(write(checkpointed.get(1), "sc_loop_joined_t2"));

    assertThat(joinedWithT1.getInputs())
        .anyMatch(ds -> ds.getName().contains("sc_t1"))
        .noneMatch(ds -> ds.getName().contains("sc_t2"));
    assertThat(joinedWithT2.getInputs())
        .anyMatch(ds -> ds.getName().contains("sc_t1"))
        .anyMatch(ds -> ds.getName().contains("sc_t2"));
  }

  /**
   * {@code df2} and {@code df4} are eager, {@code df3} in between is lazy. {@code df4}'s checkpoint
   * job computes {@code df3}'s RDD, but Spark only checkpoints the job's final RDD (unless {@code
   * spark.checkpoint.checkpointAllMarkedAncestors} is set), and once {@code df4} is checkpointed
   * {@code df3}'s RDD is no longer computed through it - so {@code df3}'s checkpoint is never
   * materialized and {@code df4}'s captured plan reads a not yet checkpointed {@code LogicalRDD}.
   */
  @Test
  void testLazyCheckpointWithinEagerChain() {
    Dataset<Row> df2 =
        spark.sql("SELECT a, b FROM sc_t1").selectExpr("a", "upper(b) AS b").checkpoint(true);
    Dataset<Row> df3 = df2.selectExpr("a", "concat(b, '_3') AS b").checkpoint(false);
    assertThat(checkpointedRdd(df3).isCheckpointed()).isFalse();

    Dataset<Row> df4 = df3.filter("a = 'x'").checkpoint(true);
    assertThat(checkpointedRdd(df3).isCheckpointed())
        .as("df4's eager checkpoint should not materialize df3's lazy checkpoint")
        .isFalse();

    List<RunEvent> events = write(df4, "sc_mixed_chain_out");
    log.info("Eager -> lazy -> eager chain:");
    events.forEach(CheckpointLineageScenariosTest::logEvent);
  }

  /**
   * Filtering keeps the output attributes - and so their {@code ExprId}s - unchanged, so two
   * checkpoints in a chain share output {@code ExprId}s. Their field-level lineage is the same, but
   * the dataset-level dependencies (the filter) differ.
   */
  @Test
  void testFilterOnlyChainSharesExprIds() {
    Dataset<Row> df2 = spark.sql("SELECT a, upper(b) AS b FROM sc_t1").checkpoint(true);
    Dataset<Row> df3 = df2.filter("a = 'x'").checkpoint(true);

    assertThat(outputExprIds(df3))
        .as("filter-only checkpoint should share output ExprIds with the previous checkpoint")
        .isEqualTo(outputExprIds(df2));

    RunEvent df2Complete = completeEvent(write(df2, "sc_filter_chain_df2"));
    RunEvent df3Complete = completeEvent(write(df3, "sc_filter_chain_df3"));

    assertThat(inputFields(df2Complete, "sc_filter_chain_df2", "b"))
        .anyMatch(f -> f.getName().contains("sc_t1") && "b".equals(f.getField()));
    assertThat(inputFields(df3Complete, "sc_filter_chain_df3", "b"))
        .as("field-level lineage is the same for both checkpoints")
        .anyMatch(f -> f.getName().contains("sc_t1") && "b".equals(f.getField()));

    assertThat(datasetDependencies(df2Complete, "sc_filter_chain_df2"))
        .as("df2 is not filtered")
        .noneMatch(f -> f.getName().contains("sc_t1") && "a".equals(f.getField()));
    assertThat(datasetDependencies(df3Complete, "sc_filter_chain_df3"))
        .as("df3 is filtered on a")
        .anyMatch(f -> f.getName().contains("sc_t1") && "a".equals(f.getField()));
  }

  /**
   * Sibling checkpoints derived from the same dataset without changing its output attributes share
   * output {@code ExprId}s, while differing in dataset-level dependencies and even input datasets.
   */
  @Test
  void testSiblingCheckpointsShareExprIds() {
    Dataset<Row> df1 = spark.sql("SELECT a, b FROM sc_t1");
    Dataset<Row> t2 = spark.sql("SELECT a AS t2_a FROM sc_t2");

    Dataset<Row> filteredOnA = df1.filter("a = 'x'").checkpoint(true);
    Dataset<Row> filteredOnB = df1.filter("b = 'y'").checkpoint(true);
    Dataset<Row> joined =
        df1.join(t2, df1.col("a").equalTo(t2.col("t2_a")))
            .select(df1.col("a"), df1.col("b"))
            .checkpoint(true);

    assertThat(outputExprIds(filteredOnA))
        .as("sibling checkpoints should share output ExprIds")
        .isEqualTo(outputExprIds(df1))
        .isEqualTo(outputExprIds(filteredOnB))
        .isEqualTo(outputExprIds(joined));

    RunEvent onA = completeEvent(write(filteredOnA, "sc_sibling_on_a"));
    RunEvent onB = completeEvent(write(filteredOnB, "sc_sibling_on_b"));
    RunEvent onJoin = completeEvent(write(joined, "sc_sibling_joined"));

    assertThat(datasetDependencies(onA, "sc_sibling_on_a"))
        .anyMatch(f -> f.getName().contains("sc_t1") && "a".equals(f.getField()))
        .noneMatch(f -> f.getName().contains("sc_t1") && "b".equals(f.getField()));
    assertThat(datasetDependencies(onB, "sc_sibling_on_b"))
        .anyMatch(f -> f.getName().contains("sc_t1") && "b".equals(f.getField()))
        .noneMatch(f -> f.getName().contains("sc_t1") && "a".equals(f.getField()));

    assertThat(onA.getInputs()).noneMatch(ds -> ds.getName().contains("sc_t2"));
    assertThat(onJoin.getInputs())
        .as("joined sibling has an extra input not reflected in its output ExprIds")
        .anyMatch(ds -> ds.getName().contains("sc_t2"));
  }

  /**
   * {@code Union} reuses the {@code ExprId}s of its first child's output, so the same {@code
   * ExprId} has different field-level lineage in the first child and in the union.
   */
  @Test
  void testUnionReusesFirstChildExprIds() {
    Dataset<Row> first = spark.sql("SELECT a, b FROM sc_t1");
    Dataset<Row> second = spark.sql("SELECT a, b FROM sc_t2");

    Dataset<Row> firstCheckpointed = first.checkpoint(true);
    Dataset<Row> unionCheckpointed = first.union(second).checkpoint(true);

    assertThat(outputExprIds(unionCheckpointed))
        .as("union should reuse ExprIds of its first child's output")
        .isEqualTo(outputExprIds(firstCheckpointed));

    RunEvent firstComplete = completeEvent(write(firstCheckpointed, "sc_union_first"));
    RunEvent unionComplete = completeEvent(write(unionCheckpointed, "sc_union_all"));

    assertThat(inputFields(firstComplete, "sc_union_first", "b"))
        .anyMatch(f -> f.getName().contains("sc_t1") && "b".equals(f.getField()))
        .noneMatch(f -> f.getName().contains("sc_t2"));
    assertThat(inputFields(unionComplete, "sc_union_all", "b"))
        .as("the same ExprId comes from both union children")
        .anyMatch(f -> f.getName().contains("sc_t1") && "b".equals(f.getField()))
        .anyMatch(f -> f.getName().contains("sc_t2") && "b".equals(f.getField()));
  }

  /** Writes the dataset to a new table and returns all events emitted for that write. */
  private List<RunEvent> write(Dataset<Row> dataset, String table) {
    String view = table + "_view";
    dataset.createOrReplaceTempView(view);
    spark.sql("CREATE TABLE " + table + " USING parquet AS SELECT * FROM " + view);
    return MockServerUtils.getEventsEmittedWithJobName(mockServer, table);
  }

  private static RunEvent completeEvent(List<RunEvent> events) {
    RunEvent complete =
        events.stream()
            .filter(e -> e.getEventType() == RunEvent.EventType.COMPLETE)
            .findFirst()
            .orElseThrow(() -> new AssertionError("No COMPLETE event found"));
    logEvent(complete);
    return complete;
  }

  private static OpenLineage.ColumnLineageDatasetFacet columnLineage(RunEvent event, String table) {
    OpenLineage.ColumnLineageDatasetFacet columnLineage =
        event.getOutputs().stream()
            .filter(ds -> ds.getName().contains(table))
            .findFirst()
            .orElseThrow(() -> new AssertionError("No output dataset for " + table))
            .getFacets()
            .getColumnLineage();
    assertThat(columnLineage).as("columnLineage facet should be present").isNotNull();
    return columnLineage;
  }

  private static List<OpenLineage.InputField> inputFields(
      RunEvent event, String table, String field) {
    OpenLineage.ColumnLineageDatasetFacetFieldsAdditional fieldLineage =
        columnLineage(event, table).getFields().getAdditionalProperties().get(field);
    assertThat(fieldLineage).as("column lineage of " + table + "." + field).isNotNull();
    return fieldLineage.getInputFields();
  }

  private static List<OpenLineage.InputField> datasetDependencies(RunEvent event, String table) {
    List<OpenLineage.InputField> dependencies = columnLineage(event, table).getDataset();
    return dependencies == null ? Collections.emptyList() : dependencies;
  }

  private static List<Long> outputExprIds(Dataset<Row> dataset) {
    return ScalaConversionUtils.<Attribute>fromSeq(dataset.queryExecution().analyzed().output())
        .stream()
        .map(attribute -> attribute.exprId().id())
        .collect(Collectors.toList());
  }

  private static org.apache.spark.rdd.RDD<?> checkpointedRdd(Dataset<Row> checkpointed) {
    LogicalPlan plan = checkpointed.queryExecution().analyzed();
    assertThat(plan)
        .as("checkpointed dataset should be backed by LogicalRDD")
        .isInstanceOf(LogicalRDD.class);
    return ((LogicalRDD) plan).rdd();
  }

  private static void logEvent(RunEvent event) {
    log.info(
        "{} {}: inputs={}, outputs={}",
        event.getEventType(),
        event.getJob().getName(),
        event.getInputs().stream().map(OpenLineage.Dataset::getName).collect(Collectors.toList()),
        event.getOutputs().stream()
            .map(
                ds ->
                    ds.getName()
                        + " columnLineage="
                        + (ds.getFacets() == null || ds.getFacets().getColumnLineage() == null
                            ? "none"
                            : OpenLineageClientUtils.toJson(ds.getFacets().getColumnLineage())))
            .collect(Collectors.toList()));
  }
}
