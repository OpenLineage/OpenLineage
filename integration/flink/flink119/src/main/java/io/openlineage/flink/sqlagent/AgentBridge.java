/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.sqlagent;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Bridges captured SQL plans to submitted Flink jobs. */
public final class AgentBridge {
  private static final Logger LOG = LoggerFactory.getLogger(AgentBridge.class);
  private static final ThreadLocal<SqlCapture> CURRENT_SQL = new ThreadLocal<>();
  private static final ThreadLocal<String> CURRENT_STATEMENT = new ThreadLocal<>();
  private static final Map<JobID, SqlRunLifecycle> RUNS = new ConcurrentHashMap<>();
  private static volatile SqlDatasetExtractor extractor;

  private AgentBridge() {}

  static void configure() {
    try {
      extractor =
          new SqlDatasetExtractor(
              System.getProperty("flink.lineage.catalog", ""),
              System.getProperty("flink.lineage.flink-catalog", ""));
    } catch (RuntimeException error) {
      LOG.error("Cannot configure Flink SQL catalog mappings", error);
      extractor = null;
    }
  }

  public static String beginStatement(String statement) {
    String previous = CURRENT_STATEMENT.get();
    if (statement == null) CURRENT_STATEMENT.remove();
    else CURRENT_STATEMENT.set(statement);
    return previous;
  }

  public static void endStatement(String previous) {
    if (previous == null) CURRENT_STATEMENT.remove();
    else CURRENT_STATEMENT.set(previous);
  }

  public static Object beginSql(Object environment, Object operations) {
    SqlCapture previous = CURRENT_SQL.get();
    CURRENT_SQL.remove();
    SqlDatasetExtractor currentExtractor = extractor;
    if (currentExtractor == null) return previous;
    try {
      Map<String, Object> datasets = currentExtractor.extract(operations);
      if (datasets != null) {
        CatalogSourceResolver.enrich(environment, datasets);
        Object tableConfig = PlannerReflection.call(environment, "getConfig");
        Configuration configuration =
            new Configuration(
                (Configuration) PlannerReflection.call(tableConfig, "getConfiguration"));
        // System properties are useful for SQL Client, which does not expose job code.
        copySystemProperties(configuration);
        CURRENT_SQL.set(new SqlCapture(datasets, configuration, CURRENT_STATEMENT.get()));
      }
    } catch (Exception | LinkageError error) {
      LOG.warn("Could not extract Flink SQL lineage", error);
    }
    return previous;
  }

  public static void endSql(Object previous) {
    if (previous == null) CURRENT_SQL.remove();
    else CURRENT_SQL.set((SqlCapture) previous);
  }

  public static void submitted(Object pipeline, Object result) {
    if (!(pipeline instanceof StreamGraph) || !(result instanceof CompletableFuture)) return;
    try {
      StreamGraph graph = (StreamGraph) pipeline;
      SqlCapture capture = CURRENT_SQL.get();
      if (capture == null) return;
      SqlRunLifecycle lifecycle =
          SqlRunLifecycle.create(capture, graph.getJobName(), graph.getJobType().name());
      ((CompletableFuture<?>) result)
          .whenComplete(
              (value, error) -> {
                if (error != null || !(value instanceof JobClient)) {
                  LOG.warn("Flink SQL submission failed before a JobClient was available", error);
                  lifecycle.close();
                  return;
                }
                startRun(lifecycle, (JobClient) value);
              });
    } catch (Exception | LinkageError error) {
      LOG.warn("Could not capture submitted Flink SQL job", error);
    }
  }

  static void startRun(SqlRunLifecycle lifecycle, JobClient client) {
    JobID jobId;
    try {
      jobId = client.getJobID();
    } catch (RuntimeException jobIdError) {
      LOG.warn("Could not get the Flink SQL job ID", jobIdError);
      lifecycle.close();
      return;
    }
    if (RUNS.putIfAbsent(jobId, lifecycle) != null) {
      lifecycle.close();
      return;
    }
    lifecycle.start(client, () -> RUNS.remove(jobId, lifecycle));
  }

  private static void copySystemProperties(Configuration configuration) {
    Properties properties = System.getProperties();
    for (String name : properties.stringPropertyNames()) {
      if (name.startsWith("openlineage.")) {
        configuration.setString(name, properties.getProperty(name));
      }
    }
  }
}
