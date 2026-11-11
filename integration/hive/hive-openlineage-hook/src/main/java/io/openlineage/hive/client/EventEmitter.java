/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.client;

import static io.openlineage.client.utils.UUIDUtils.generateStaticUUID;
import static java.nio.charset.StandardCharsets.UTF_8;

import io.openlineage.client.Clients;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.utils.UUIDUtils;
import io.openlineage.hive.api.OpenLineageContext;
import java.time.Instant;
import java.util.UUID;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.QueryInfo;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.hooks.HookContext;

@Getter
@Slf4j
public class EventEmitter implements AutoCloseable {
  private final OpenLineageClient client;
  private final UUID runId;
  private final String jobName;
  private final String jobNamespace;

  public EventEmitter(OpenLineageContext olContext) {
    Configuration conf = olContext.getHookContext().getConf();
    this.client = Clients.newClient(olContext.getOpenLineageConfig());
    this.runId = getRunId(olContext);
    this.jobNamespace = conf.get(HiveOpenLineageConfigParser.NAMESPACE_KEY, "default");
    this.jobName = conf.get(HiveOpenLineageConfigParser.JOB_NAME_KEY, getJobName(olContext));
  }

  public void emit(RunEvent event) {
    try {
      client.emit(event);
      log.debug(
          "Emitting lineage completed successfully: {}", OpenLineageClientUtils.toJson(event));
    } catch (OpenLineageClientException exception) {
      log.error("Could not emit lineage", exception);
    }
  }

  public static String getJobName(OpenLineageContext olContext) {
    return olContext.getHookContext().getOperationName();
  }

  /*
   * Get the same runId for START and STOP events
   */
  public static UUID getRunId(OpenLineageContext olContext) {
    HookContext hookContext = olContext.getHookContext();
    // QueryInfo is populated only by HiveServer2, it is null when running via Hive CLI
    // https://github.com/apache/hive/blob/rel/release-3.1.3/ql/src/java/org/apache/hadoop/hive/ql/QueryInfo.java
    QueryInfo queryInfo = hookContext.getQueryInfo();
    if (queryInfo != null && queryInfo.getOperationId() != null) {
      return generateStaticUUID(
          Instant.ofEpochMilli(queryInfo.getBeginTime()),
          queryInfo.getOperationId().getBytes(UTF_8));
    }

    QueryPlan queryPlan = hookContext.getQueryPlan();
    if (queryPlan != null
        && queryPlan.getQueryId() != null
        && queryPlan.getQueryStartTime() != null) {
      return generateStaticUUID(
          Instant.ofEpochMilli(queryPlan.getQueryStartTime()),
          queryPlan.getQueryId().getBytes(UTF_8));
    }

    log.warn("Unable to determine query id and start time, generating random runId");
    return UUIDUtils.generateNewUUID();
  }

  @Override
  public void close() throws Exception {
    client.close();
  }
}
