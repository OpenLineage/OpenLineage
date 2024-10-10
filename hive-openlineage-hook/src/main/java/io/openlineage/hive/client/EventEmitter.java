/*
 * Copyright 2024 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openlineage.hive.client;

import static com.google.common.hash.Hashing.sha512;

import io.openlineage.client.Clients;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.hive.api.OpenLineageContext;
import io.openlineage.hive.util.NetworkUtils;
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
    this.runId = UUID.randomUUID();
    this.jobNamespace =
        conf.get(
            HiveOpenLineageConfigParser.NAMESPACE_KEY, getJobNamespace(olContext.getQueryString()));
    this.jobName =
        conf.get(
            HiveOpenLineageConfigParser.JOB_NAME_KEY, NetworkUtils.LOCAL_IP_ADDRESS.getHostName());
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

  public static String getJobNamespace(String queryString) {
    // TODO: Confirm that this is an appropriate hashing function to use
    return sha512().hashUnencodedChars(queryString).toString();
  }

  @Override
  public void close() throws Exception {
    client.close();
  }
}
