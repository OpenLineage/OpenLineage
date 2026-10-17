/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.spark.agent.EventEmitter;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.CommandExecutionMode$;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import org.apache.spark.sql.internal.SessionState;
import org.mockito.ArgumentCaptor;

/**
 * Runs a SQL command through the production {@link ContextFactory} and {@link
 * SparkSQLExecutionContext} and describes the datasets of emitted events as comparable lines.
 *
 * <p>Events are produced synchronously: START is built before the command executes and COMPLETE
 * after it. Going through the listener and Spark's asynchronous listener bus is not deterministic,
 * as Spark may drop the query execution of a short command before the listener handles its start
 * event.
 *
 * <p>Each line has the format {@code <eventType> <jobName> <IN|OUT> <namespace>:<name>
 * [lifecycleStateChange] [schema=<fields>] facets=<facet names>}. Per-run directories are replaced
 * with {@code <wh>} (warehouse), {@code <tmp>} and {@code <derby>} (Derby system home)
 * placeholders, and with {@code WH} and {@code TMP} in job names, which are built from the last two
 * path segments.
 */
class SqlCommandLineage {
  private final SparkSession spark;
  private final String warehouse;
  private final Path tmp;

  SqlCommandLineage(SparkSession spark, Path tmp) {
    this.spark = spark;
    this.warehouse = spark.conf().get("spark.sql.warehouse.dir").replace("file:", "");
    this.tmp = tmp;
  }

  /** Compares the {@code major.minor} part of the running Spark version. */
  boolean isSparkAtLeast(int major, int minor) {
    String[] parts = spark.version().split("\\.");
    int actualMajor = Integer.parseInt(parts[0]);
    int actualMinor = Integer.parseInt(parts[1]);
    return actualMajor > major || (actualMajor == major && actualMinor >= minor);
  }

  @SneakyThrows
  List<String> run(String sql) {
    SessionState sessionState = (SessionState) MethodUtils.invokeMethod(spark, "sessionState");
    LogicalPlan plan = sessionState.sqlParser().parsePlan(sql);
    // NON_ROOT is the mode Spark uses for the query execution of an eagerly executed command,
    // which is the one OpenLineage receives for it
    QueryExecution qe = sessionState.executePlan(plan, CommandExecutionMode$.MODULE$.NON_ROOT());

    EventEmitter emitter = emitter();
    SparkListenerSQLExecutionStart start = mock(SparkListenerSQLExecutionStart.class);
    when(start.time()).thenReturn(System.currentTimeMillis());
    SparkListenerSQLExecutionEnd end = mock(SparkListenerSQLExecutionEnd.class);
    when(end.time()).thenReturn(System.currentTimeMillis());
    when(end.qe()).thenReturn(qe);

    ExecutionContext context =
        new ContextFactory(emitter, new SimpleMeterRegistry(), new SparkOpenLineageConfig())
            .createSparkSQLExecutionContext(end)
            .orElseThrow(() -> new IllegalStateException("No execution context for " + sql));
    context.start(start);
    qe.executedPlan().executeCollect();
    context.end(end);

    ArgumentCaptor<RunEvent> captor = ArgumentCaptor.forClass(RunEvent.class);
    verify(emitter, atLeast(0)).emit(captor.capture());
    TreeSet<String> lines = new TreeSet<>();
    for (RunEvent event : captor.getAllValues()) {
      String prefix = event.getEventType() + " " + normalize(event.getJob().getName()) + " ";
      event.getInputs().forEach(ds -> lines.add(prefix + "IN " + describe(ds)));
      event.getOutputs().forEach(ds -> lines.add(prefix + "OUT " + describe(ds)));
    }
    return new ArrayList<>(lines);
  }

  private EventEmitter emitter() {
    EventEmitter emitter = mock(EventEmitter.class);
    when(emitter.getJobNamespace()).thenReturn("ns_name");
    when(emitter.getParentJobName()).thenReturn(Optional.empty());
    when(emitter.getParentRunId()).thenReturn(Optional.empty());
    when(emitter.getApplicationRunId()).thenReturn(UUID.randomUUID());
    when(emitter.getApplicationJobName()).thenReturn("app");
    when(emitter.getCustomEnvironmentVariables()).thenReturn(Optional.empty());
    return emitter;
  }

  private String describe(OpenLineage.Dataset ds) {
    OpenLineage.DatasetFacets facets = ds.getFacets();
    String lifecycle =
        Optional.ofNullable(facets.getLifecycleStateChange())
            .map(f -> " " + f.getLifecycleStateChange())
            .orElse("");
    String schema =
        Optional.ofNullable(facets.getSchema())
            .map(
                s ->
                    " schema="
                        + s.getFields().stream()
                            .map(OpenLineage.SchemaDatasetFacetFields::getName)
                            .collect(Collectors.joining(",")))
            .orElse("");
    return normalize(ds.getNamespace() + ":" + ds.getName())
        + lifecycle
        + schema
        + " facets="
        + facetNames(facets);
  }

  @SneakyThrows
  private static String facetNames(OpenLineage.DatasetFacets facets) {
    // serialized with the client's own (possibly shaded) mapper to respect facet annotations
    JsonNode node = new ObjectMapper().readTree(OpenLineageClientUtils.toJson(facets));
    List<String> names = new ArrayList<>();
    node.fieldNames().forEachRemaining(names::add);
    Collections.sort(names);
    return String.join(",", names);
  }

  @SneakyThrows
  private String normalize(String value) {
    String derbyHome = System.getProperty("derby.system.home");
    String result = derbyHome == null ? value : value.replace(derbyHome, "<derby>");
    return result
        .replace(tmp.toRealPath().toString(), "<tmp>")
        .replace(tmp.toString(), "<tmp>")
        .replace(warehouse, "<wh>")
        .replace(tmp.getFileName().toString(), "TMP")
        .replace(Paths.get(warehouse).getFileName().toString(), "WH");
  }
}
