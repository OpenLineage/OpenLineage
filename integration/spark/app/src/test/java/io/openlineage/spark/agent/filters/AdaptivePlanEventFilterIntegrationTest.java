/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.filters;

import com.google.common.collect.ImmutableList;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Spark4CompatUtils;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.lifecycle.SparkOpenLineageExtensionVisitorWrapper;
import io.openlineage.spark.agent.util.SparkSqlExecutionNestingTracker;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.Command;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SQLExecution;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import org.apache.spark.sql.types.LongType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration-test")
@Tag("delta")
class AdaptivePlanEventFilterIntegrationTest {

  private static final String TEST_ROOT = "/tmp/delta-adaptive-filter-integration";
  private static SparkSession spark;
  private static QueryExecutionCaptureListener captureListener;

  @BeforeAll
  @SneakyThrows
  static void beforeAll() {
    Spark4CompatUtils.cleanupAnyExistingSession();
    FileUtils.deleteDirectory(new File(TEST_ROOT));
    System.setProperty("derby.system.home", TEST_ROOT + "/derby");

    spark =
        SparkSession.builder()
            .master("local[*]")
            .appName("AdaptivePlanEventFilterIntegrationTest")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.ui.enabled", false)
            .config("spark.sql.adaptive.enabled", true)
            .config("spark.sql.shuffle.partitions", 1)
            .config("spark.sql.warehouse.dir", "file:" + TEST_ROOT)
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .getOrCreate();

    captureListener = new QueryExecutionCaptureListener();
    spark.sparkContext().addSparkListener(captureListener);
  }

  @AfterAll
  @SneakyThrows
  static void afterAll() {
    if (spark != null && captureListener != null) {
      spark.sparkContext().removeSparkListener(captureListener);
    }
    Spark4CompatUtils.cleanupAnyExistingSession();
    FileUtils.deleteDirectory(new File(TEST_ROOT));
  }

  @Test
  void testRealDeltaAdaptiveChildrenAreFiltered() {
    clearTables(
        "pair_append",
        "pair_overwrite",
        "pair_left",
        "pair_right",
        "pair_ctas",
        "pair_rtas",
        "pair_merge_source",
        "pair_merge_target");

    createDeltaTable("pair_append");
    createDeltaTable("pair_overwrite");
    createDeltaTable("pair_left");
    createDeltaTable("pair_right");
    createDeltaTable("pair_rtas");
    createDeltaTable("pair_merge_source");
    createDeltaTable("pair_merge_target");

    List<OperationExecutions> operations = new ArrayList<>();
    operations.add(
        observe(
            "path save",
            () ->
                rows()
                    .repartition(1)
                    .write()
                    .format("delta")
                    .mode("overwrite")
                    .save(TEST_ROOT + "/pair_path")));
    operations.add(
        observe(
            "append",
            () ->
                rows()
                    .repartition(1)
                    .write()
                    .format("delta")
                    .mode("append")
                    .saveAsTable("pair_append")));
    operations.add(
        observe(
            "overwrite",
            () ->
                rows()
                    .repartition(1)
                    .write()
                    .format("delta")
                    .mode("overwrite")
                    .saveAsTable("pair_overwrite")));
    operations.add(
        observe(
            "CTAS",
            () ->
                spark.sql(
                    "CREATE TABLE pair_ctas USING delta AS "
                        + "SELECT l.a, r.b FROM pair_left l JOIN pair_right r ON l.a = r.a")));
    operations.add(
        observe(
            "RTAS",
            () ->
                spark.sql(
                    "REPLACE TABLE pair_rtas USING delta AS "
                        + "SELECT l.a, r.b FROM pair_left l JOIN pair_right r ON l.a = r.a")));
    operations.add(
        observe(
            "MERGE",
            () ->
                spark.sql(
                    "MERGE INTO pair_merge_target t USING pair_merge_source s ON t.a = s.a "
                        + "WHEN MATCHED THEN UPDATE SET t.b = s.b "
                        + "WHEN NOT MATCHED THEN INSERT *")));

    SoftAssertions softly = new SoftAssertions();
    operations.forEach(
        operation -> {
          softly
              .assertThat(operation.executions)
              .as("%s must launch an adaptive execution: %s", operation.name, operation.summary())
              .anyMatch(execution -> execution.adaptive);
          softly
              .assertThat(operation.executions)
              .filteredOn(
                  execution ->
                      execution.commandChildExecution && !execution.deltaEventFilterDisabled)
              .as(
                  "%s child executions not handled by DeltaEventFilter must be handled by "
                      + "AdaptivePlanEventFilter: %s",
                  operation.name, operation.summary())
              .allMatch(execution -> execution.adaptiveFilterDisabled);
          softly
              .assertThat(operation.executions)
              .filteredOn(execution -> execution.adaptive)
              .as(
                  "%s adaptive children must be correlated as nested: %s",
                  operation.name, operation.summary())
              .allMatch(execution -> execution.nestedExecution);
          softly
              .assertThat(operation.executions)
              .filteredOn(execution -> execution.adaptive)
              .as(
                  "%s adaptive children must be tied to a command root: %s",
                  operation.name, operation.summary())
              .allMatch(execution -> execution.commandChildExecution);

          if (operation.hasRootExecutionIds()) {
            softly
                .assertThat(operation.executions)
                .filteredOn(execution -> execution.adaptive)
                .as(
                    "%s adaptive executions must be nested under the observed top-level "
                        + "execution: %s",
                    operation.name, operation.summary())
                .allMatch(
                    execution ->
                        execution.rootExecutionId.isPresent()
                            && execution.rootExecutionId.getAsLong() != execution.executionId);
          }
        });
    softly.assertAll();
  }

  @Test
  void testDeltaCtasAndRtasHaveNonAdaptiveOuterAndAdaptiveInnerExecutions() {
    clearTables("classifier_source", "classifier_ctas", "classifier_rtas");
    createDeltaTable("classifier_source");
    createDeltaTable("classifier_rtas");

    OperationExecutions ctas =
        observe(
            "CTAS classifier",
            () ->
                spark.sql(
                    "CREATE TABLE classifier_ctas USING DeLtA AS "
                        + "SELECT * FROM classifier_source"));
    OperationExecutions rtas =
        observe(
            "RTAS classifier",
            () ->
                spark.sql(
                    "REPLACE TABLE classifier_rtas USING DeLtA AS "
                        + "SELECT * FROM classifier_source"));

    SoftAssertions softly = new SoftAssertions();
    assertNonAdaptiveOuterWithAdaptiveInner(softly, ctas, "CreateTableAsSelect");
    assertNonAdaptiveOuterWithAdaptiveInner(softly, rtas, "ReplaceTableAsSelect");
    softly.assertAll();
  }

  @Test
  void testNonDeltaCtasKeepsTopLevelOuterExecutionUnderDeltaCatalog() {
    clearTables(
        "classifier_parquet_source",
        "classifier_parquet_ctas",
        "classifier_orc_source",
        "classifier_orc_ctas");
    rows().write().format("parquet").saveAsTable("classifier_parquet_source");
    rows().write().format("orc").saveAsTable("classifier_orc_source");

    OperationExecutions parquet =
        observe(
            "Parquet CTAS classifier",
            () ->
                spark.sql(
                    "CREATE TABLE classifier_parquet_ctas USING parquet AS "
                        + "SELECT * FROM classifier_parquet_source"));
    OperationExecutions orc =
        observe(
            "ORC CTAS classifier",
            () ->
                spark.sql(
                    "CREATE TABLE classifier_orc_ctas USING orc AS "
                        + "SELECT * FROM classifier_orc_source"));

    SoftAssertions softly = new SoftAssertions();
    assertCtasKeepsOuterExecution(softly, parquet);
    assertCtasKeepsOuterExecution(softly, orc);
    softly.assertAll();
  }

  @Test
  void testDeltaEventFilterKeepsTopLevelNonDeltaPlans() {
    clearTables("non_delta_filter_source");
    rows().write().format("parquet").saveAsTable("non_delta_filter_source");

    OperationExecutions filter =
        observe(
            "Filter",
            () -> spark.sql("SELECT * FROM non_delta_filter_source WHERE a > 1").collectAsList());
    OperationExecutions localRelation =
        observe("LocalRelation", () -> spark.sql("VALUES (1), (2)").collectAsList());
    OperationExecutions serializeFromObject =
        observe(
            "SerializeFromObject",
            () ->
                spark
                    .createDataset(Arrays.asList("a", "b"), Encoders.STRING())
                    .map((MapFunction<String, String>) value -> value + "x", Encoders.STRING())
                    .collectAsList());

    SoftAssertions softly = new SoftAssertions();
    assertTopLevelPlanIsKept(softly, filter, "Filter");
    assertTopLevelPlanIsKept(softly, localRelation, "LocalRelation");
    assertTopLevelPlanIsKept(softly, serializeFromObject, "SerializeFromObject");
    softly.assertAll();
  }

  private static void assertTopLevelPlanIsKept(
      SoftAssertions softly, OperationExecutions operation, String optimizedRoot) {
    java.util.Optional<ObservedExecution> matchingExecution =
        operation.executionWithOptimizedRoot(optimizedRoot);
    softly
        .assertThat(matchingExecution)
        .as("Expected a real %s root: %s", optimizedRoot, operation.summary())
        .isPresent();
    matchingExecution.ifPresent(
        execution ->
            softly
                .assertThat(execution.deltaEventFilterDisabled)
                .as(
                    "A top-level %s must not be treated as a Delta-internal execution",
                    optimizedRoot)
                .isFalse());
  }

  private static void assertCtasKeepsOuterExecution(
      SoftAssertions softly, OperationExecutions operation) {
    java.util.Optional<ObservedExecution> ctas =
        operation.executionWithOptimizedRootContaining("TableAsSelect");
    softly
        .assertThat(ctas)
        .as("Expected a real non-Delta CTAS root: %s", operation.summary())
        .isPresent();
    ctas.ifPresent(
        execution -> {
          softly
              .assertThat(execution.nestedExecution)
              .as("A non-Delta CTAS outer execution must remain top-level: %s", operation.name)
              .isFalse();
          softly
              .assertThat(execution.adaptiveFilterDisabled)
              .as("A non-Delta CTAS outer execution must not be filtered: %s", operation.name)
              .isFalse();
          softly
              .assertThat(execution.commandRoot)
              .as("A non-Delta CTAS outer execution is still a root command")
              .isTrue();
        });
    softly
        .assertThat(operation.executions)
        .filteredOn(execution -> execution.nestedExecution)
        .as("Nested non-Delta CTAS work must be deduplicated: %s", operation.summary())
        .isNotEmpty()
        .allSatisfy(
            execution -> {
              softly.assertThat(execution.commandChildExecution).isTrue();
              softly.assertThat(execution.adaptiveFilterDisabled).isTrue();
            });
  }

  private static void assertNonAdaptiveOuterWithAdaptiveInner(
      SoftAssertions softly, OperationExecutions operation, String optimizedRoot) {
    java.util.Optional<ObservedExecution> execution =
        operation.executionWithOptimizedRoot(optimizedRoot);
    softly
        .assertThat(execution)
        .as("Expected a real %s root: %s", optimizedRoot, operation.summary())
        .isPresent();
    execution.ifPresent(
        value -> {
          softly
              .assertThat(value.adaptive)
              .as("The real %s outer execution must not itself be adaptive", optimizedRoot)
              .isFalse();
          softly
              .assertThat(value.commandRoot)
              .as("The real %s outer execution must be classified as a command", optimizedRoot)
              .isTrue();
        });
    softly
        .assertThat(operation.executions)
        .as("The real %s operation must launch an adaptive inner execution", optimizedRoot)
        .anyMatch(value -> value.adaptive);
  }

  private static OperationExecutions observe(String name, Runnable operation) {
    drainListenerBus();
    captureListener.clear();
    operation.run();
    drainListenerBus();

    List<ObservedExecution> executions =
        captureListener.snapshot().stream()
            .map(AdaptivePlanEventFilterIntegrationTest::inspect)
            .collect(Collectors.toList());
    return new OperationExecutions(name, executions);
  }

  private static ObservedExecution inspect(CapturedExecution execution) {
    SparkOpenLineageConfig config = new SparkOpenLineageConfig();
    OpenLineageContext context =
        OpenLineageContext.builder()
            .sparkSession(spark)
            .sparkContext(spark.sparkContext())
            .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
            .queryExecution(execution.queryExecution)
            .commandChildExecution(execution.commandChildExecution)
            .meterRegistry(new SimpleMeterRegistry())
            .openLineageConfig(config)
            .sparkExtensionVisitorWrapper(new SparkOpenLineageExtensionVisitorWrapper(config))
            .build();

    return new ObservedExecution(
        execution.executionId,
        execution.rootExecutionId,
        execution.queryExecution.optimizedPlan().getClass().getSimpleName(),
        execution.queryExecution.executedPlan().nodeName(),
        execution.nestedExecution,
        execution.commandChildExecution,
        execution.queryExecution.optimizedPlan() instanceof Command,
        execution.queryExecution.executedPlan().nodeName().contains("AdaptiveSparkPlan"),
        new AdaptivePlanEventFilter(context).isDisabled(execution.endEvent),
        new DeltaEventFilter(context).isDisabled(execution.endEvent));
  }

  private static Dataset<Row> rows() {
    return spark.createDataFrame(
        ImmutableList.of(RowFactory.create(1L, "bat"), RowFactory.create(3L, "horse")),
        new StructType(
            new StructField[] {
              new StructField("a", LongType$.MODULE$, false, Metadata.empty()),
              new StructField("b", StringType$.MODULE$, false, Metadata.empty())
            }));
  }

  private static void createDeltaTable(String table) {
    rows().repartition(1).write().format("delta").saveAsTable(table);
    drainListenerBus();
  }

  private static void clearTables(String... tables) {
    Arrays.stream(tables).forEach(table -> spark.sql("DROP TABLE IF EXISTS " + table));
    drainListenerBus();
  }

  @SneakyThrows
  private static void drainListenerBus() {
    spark.sparkContext().listenerBus().waitUntilEmpty(10_000);
  }

  @RequiredArgsConstructor
  private static class OperationExecutions {
    private final String name;
    private final List<ObservedExecution> executions;

    private boolean hasRootExecutionIds() {
      return executions.stream().anyMatch(execution -> execution.rootExecutionId.isPresent());
    }

    private java.util.Optional<ObservedExecution> executionWithOptimizedRoot(String root) {
      return executions.stream()
          .filter(execution -> root.equals(execution.optimizedRoot))
          .findFirst();
    }

    private java.util.Optional<ObservedExecution> executionWithOptimizedRootContaining(
        String fragment) {
      return executions.stream()
          .filter(execution -> execution.optimizedRoot.contains(fragment))
          .findFirst();
    }

    private String summary() {
      return executions.stream().map(ObservedExecution::toString).collect(Collectors.joining(", "));
    }
  }

  @RequiredArgsConstructor
  private static class ObservedExecution {
    private final long executionId;
    private final OptionalLong rootExecutionId;
    private final String optimizedRoot;
    private final String executedRoot;
    private final boolean nestedExecution;
    private final boolean commandChildExecution;
    private final boolean commandRoot;
    private final boolean adaptive;
    private final boolean adaptiveFilterDisabled;
    private final boolean deltaEventFilterDisabled;

    @Override
    public String toString() {
      return String.format(
          "executionId=%d rootExecutionId=%s optimized=%s executed=%s nested=%s "
              + "commandChild=%s commandRoot=%s adaptive=%s adaptiveFiltered=%s "
              + "deltaFiltered=%s",
          executionId,
          rootExecutionId.isPresent() ? Long.toString(rootExecutionId.getAsLong()) : "absent",
          optimizedRoot,
          executedRoot,
          nestedExecution,
          commandChildExecution,
          commandRoot,
          adaptive,
          adaptiveFilterDisabled,
          deltaEventFilterDisabled);
    }
  }

  @RequiredArgsConstructor
  private static class CapturedExecution {
    private final long executionId;
    private final OptionalLong rootExecutionId;
    private final QueryExecution queryExecution;
    private final SparkListenerSQLExecutionEnd endEvent;
    private final boolean nestedExecution;
    private final boolean commandChildExecution;
  }

  private static class QueryExecutionCaptureListener extends SparkListener {
    private final Map<Long, OptionalLong> rootExecutionIds = new ConcurrentHashMap<>();
    private final Map<Long, Boolean> nestedExecutions = new ConcurrentHashMap<>();
    private final Map<Long, Boolean> commandChildExecutions = new ConcurrentHashMap<>();
    private final List<CapturedExecution> executions = new CopyOnWriteArrayList<>();
    private final SparkSqlExecutionNestingTracker nestingTracker =
        new SparkSqlExecutionNestingTracker();

    @Override
    public void onOtherEvent(SparkListenerEvent event) {
      if (event instanceof SparkListenerSQLExecutionStart) {
        SparkListenerSQLExecutionStart start = (SparkListenerSQLExecutionStart) event;
        rootExecutionIds.put(
            start.executionId(), SparkSqlExecutionNestingTracker.rootExecutionId(start));
        nestedExecutions.put(
            start.executionId(),
            nestingTracker.register(start, SQLExecution.getQueryExecution(start.executionId())));
        commandChildExecutions.put(
            start.executionId(), nestingTracker.isCommandChild(start.executionId()));
      } else if (event instanceof SparkListenerSQLExecutionEnd) {
        SparkListenerSQLExecutionEnd end = (SparkListenerSQLExecutionEnd) event;
        try {
          if (end.qe() != null) {
            executions.add(
                new CapturedExecution(
                    end.executionId(),
                    rootExecutionIds.getOrDefault(end.executionId(), OptionalLong.empty()),
                    end.qe(),
                    end,
                    nestedExecutions.getOrDefault(end.executionId(), false),
                    commandChildExecutions.getOrDefault(end.executionId(), false)));
          }
        } finally {
          nestingTracker.end(end.executionId());
        }
      }
    }

    private void clear() {
      rootExecutionIds.clear();
      nestedExecutions.clear();
      commandChildExecutions.clear();
      executions.clear();
      nestingTracker.clear();
    }

    private List<CapturedExecution> snapshot() {
      return Collections.unmodifiableList(new ArrayList<>(executions));
    }
  }
}
