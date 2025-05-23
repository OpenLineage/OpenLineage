/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.client;

import static io.openlineage.client.utils.UUIDUtils.generateNewUUID;

import io.openlineage.client.Clients;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.hive.api.OpenLineageContext;
import java.util.UUID;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;

@Getter
@Slf4j
public class EventEmitter implements AutoCloseable {
  private final OpenLineageClient client;
  private final UUID runId;
  private final String jobName;
  private final String jobNamespace;

  public EventEmitter(OpenLineageContext olContext) {
    Configuration conf = olContext.getHadoopConf();
    this.client = Clients.newClient(olContext.getOpenLineageConfig());
    this.runId = generateNewUUID();
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

  public static String getJobName(OpenLineageContext hookContext) {
    return hookContext.getOperationName();
  }

  @Override
  public void close() throws Exception {
    client.close();
  }
}
