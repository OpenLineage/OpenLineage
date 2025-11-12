/*
/* Copyright 2018-2025 contributors to the OpenLineage project
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
import io.openlineage.hive.api.OpenLineageContext;
import java.time.Instant;
import java.util.UUID;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.QueryInfo;

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
    // https://github.com/apache/hive/blob/rel/release-3.1.3/ql/src/java/org/apache/hadoop/hive/ql/QueryInfo.java
    QueryInfo query = olContext.getHookContext().getQueryInfo();
    Instant beginTime = Instant.ofEpochMilli(query.getBeginTime());
    String operationId = query.getOperationId();
    return generateStaticUUID(beginTime, operationId.getBytes(UTF_8));
  }

  @Override
  public void close() throws Exception {
    client.close();
  }
}
