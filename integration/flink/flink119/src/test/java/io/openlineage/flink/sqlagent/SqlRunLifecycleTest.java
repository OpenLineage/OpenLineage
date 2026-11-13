/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.sqlagent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SqlRunLifecycleTest {
  @TempDir Path directory;

  @Test
  void gatewayStatementScopeRestoresPreviousSql() {
    assertNull(GatewayStatementAdvice.enter("INSERT INTO first SELECT * FROM source"));
    try {
      String previous = GatewayStatementAdvice.enter("INSERT INTO second SELECT * FROM source");
      assertEquals("INSERT INTO first SELECT * FROM source", previous);
      GatewayStatementAdvice.exit(previous);
      assertEquals("INSERT INTO first SELECT * FROM source", GatewayStatementAdvice.enter(null));
      GatewayStatementAdvice.exit("INSERT INTO first SELECT * FROM source");
    } finally {
      GatewayStatementAdvice.exit(null);
    }
    assertNull(GatewayStatementAdvice.enter("INSERT INTO third SELECT * FROM source"));
    GatewayStatementAdvice.exit(null);
  }

  @Test
  void emitsOriginalSqlOnStartAndComplete() throws Exception {
    Path events = directory.resolve("sql-events");
    Configuration configuration = new Configuration();
    configuration.setString("openlineage.transport.type", "file");
    configuration.setString("openlineage.transport.location", events.toString());
    String statement = "INSERT INTO sink SELECT id FROM source";
    SqlCapture capture =
        new SqlCapture(
            Map.of("outputs", List.of(Map.of("namespace", "catalog", "name", "db.sink"))),
            configuration,
            statement);
    SqlRunLifecycle lifecycle = SqlRunLifecycle.create(capture, "insert_sink", "BATCH");
    lifecycle.start(
        (JobClient)
            Proxy.newProxyInstance(
                JobClient.class.getClassLoader(),
                new Class<?>[] {JobClient.class},
                (proxy, method, args) -> {
                  if (method.getName().equals("getJobID")) return new JobID();
                  if (method.getName().equals("getJobExecutionResult"))
                    return CompletableFuture.completedFuture(null);
                  throw new UnsupportedOperationException(method.getName());
                }),
        () -> {});

    List<String> emitted = Files.readAllLines(events);
    assertEquals(2, emitted.size());
    for (String line : emitted) {
      OpenLineage.RunEvent event = OpenLineageClientUtils.runEventFromJson(line);
      assertEquals(statement, event.getJob().getFacets().getSql().getQuery());
      assertEquals("flink", event.getJob().getFacets().getSql().getDialect());
    }
  }

  @Test
  void defaultAndExplicitNamespacesOnlyAffectJobIdentity() throws Exception {
    for (String configured : new String[] {null, "custom-flink", "default"}) {
      Path events = directory.resolve(configured == null ? "unset" : configured);
      Configuration configuration = new Configuration();
      configuration.setString("openlineage.transport.type", "file");
      configuration.setString("openlineage.transport.location", events.toString());
      if (configured != null) configuration.setString("openlineage.job.namespace", configured);
      SqlCapture capture =
          new SqlCapture(
              Map.of(
                  "outputs",
                  List.of(Map.of("namespace", "dataset-source", "name", "sales.orders"))),
              configuration);
      SqlRunLifecycle lifecycle = SqlRunLifecycle.create(capture, "insert_orders", "BATCH");
      JobClient client =
          (JobClient)
              Proxy.newProxyInstance(
                  JobClient.class.getClassLoader(),
                  new Class<?>[] {JobClient.class},
                  (proxy, method, args) -> {
                    if (method.getName().equals("getJobID")) return new JobID();
                    if (method.getName().equals("getJobExecutionResult"))
                      return new CompletableFuture<>();
                    throw new UnsupportedOperationException(method.getName());
                  });
      try {
        lifecycle.start(client, () -> {});
        OpenLineage.RunEvent event =
            OpenLineageClientUtils.runEventFromJson(Files.readAllLines(events).get(0));
        assertEquals(configured == null ? "flink-jobs" : configured, event.getJob().getNamespace());
        assertEquals("insert_orders", event.getJob().getName());
        assertEquals("dataset-source", event.getOutputs().get(0).getNamespace());
        assertEquals("sales.orders", event.getOutputs().get(0).getName());
      } finally {
        lifecycle.close();
      }
    }
  }

  @Test
  void failedFallbackEmissionStillClosesAndRemovesRun() throws Exception {
    Path events = directory.resolve("fallback-events");
    SqlRunLifecycle lifecycle = createLifecycle(events);
    JobID jobId = new JobID();
    RuntimeException resultError =
        new RuntimeException("result unavailable") {
          @Override
          public String toString() {
            throw new IllegalStateException("error could not be formatted");
          }
        };
    JobClient client =
        jobClient(
            (proxy, method, args) -> {
              if (method.getName().equals("getJobID")) return jobId;
              if (method.getName().equals("getJobExecutionResult")) throw resultError;
              throw new UnsupportedOperationException(method.getName());
            });

    AgentBridge.startRun(lifecycle, client);

    assertTrue(isClosed(lifecycle));
    assertTrue(runs().get(jobId) == null);
    assertEquals(1, Files.readAllLines(events).size());
  }

  @Test
  void firstJobIdFailureClosesEmitter() throws Exception {
    SqlRunLifecycle lifecycle = createLifecycle(directory.resolve("first-id-failure"));
    int previousRunCount = runs().size();
    JobClient client =
        jobClient(
            (proxy, method, args) -> {
              if (method.getName().equals("getJobID"))
                throw new IllegalStateException("job ID unavailable");
              throw new UnsupportedOperationException(method.getName());
            });

    AgentBridge.startRun(lifecycle, client);

    assertTrue(isClosed(lifecycle));
    assertEquals(previousRunCount, runs().size());
  }

  @Test
  void secondJobIdFailureRemovesRegisteredRun() throws Exception {
    SqlRunLifecycle lifecycle = createLifecycle(directory.resolve("second-id-failure"));
    JobID jobId = new JobID();
    AtomicInteger calls = new AtomicInteger();
    JobClient client =
        jobClient(
            (proxy, method, args) -> {
              if (method.getName().equals("getJobID")) {
                if (calls.incrementAndGet() == 1) return jobId;
                throw new IllegalStateException("job ID unavailable on retry");
              }
              throw new UnsupportedOperationException(method.getName());
            });

    AgentBridge.startRun(lifecycle, client);

    assertEquals(2, calls.get());
    assertTrue(isClosed(lifecycle));
    assertTrue(runs().get(jobId) == null);
  }

  private SqlRunLifecycle createLifecycle(Path events) {
    Configuration configuration = new Configuration();
    configuration.setString("openlineage.transport.type", "file");
    configuration.setString("openlineage.transport.location", events.toString());
    SqlCapture capture =
        new SqlCapture(
            Map.of("outputs", List.of(Map.of("namespace", "catalog", "name", "db.sink"))),
            configuration);
    return SqlRunLifecycle.create(capture, "insert_sink", "BATCH");
  }

  private static JobClient jobClient(java.lang.reflect.InvocationHandler handler) {
    return (JobClient)
        Proxy.newProxyInstance(
            JobClient.class.getClassLoader(), new Class<?>[] {JobClient.class}, handler);
  }

  private static boolean isClosed(SqlRunLifecycle lifecycle) throws Exception {
    Field field = SqlRunLifecycle.class.getDeclaredField("closed");
    field.setAccessible(true);
    return ((AtomicBoolean) field.get(lifecycle)).get();
  }

  @SuppressWarnings("unchecked")
  private static Map<JobID, SqlRunLifecycle> runs() throws Exception {
    Field field = AgentBridge.class.getDeclaredField("RUNS");
    field.setAccessible(true);
    return (Map<JobID, SqlRunLifecycle>) field.get(null);
  }
}
