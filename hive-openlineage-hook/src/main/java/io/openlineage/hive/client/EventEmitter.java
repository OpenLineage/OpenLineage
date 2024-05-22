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

import io.openlineage.client.*;
import io.openlineage.client.circuitBreaker.CircuitBreakerFactory;
import io.openlineage.client.transports.FacetsConfig;
import io.openlineage.client.transports.TransportFactory;
import java.util.Optional;
import java.util.UUID;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;

@Slf4j
public class EventEmitter {
  @Getter private OpenLineageClient client;
  @Getter private UUID runId;
  @Getter private String jobName;
  @Getter private String jobNamespace;

  public EventEmitter(Configuration conf) {
    OpenLineageYaml openLineageYaml = HiveOpenLineageConfigParser.extractOpenLineageYaml(conf);
    String[] disabledFacets =
        Optional.ofNullable(openLineageYaml.getFacetsConfig())
            .orElse(new FacetsConfig().withDisabledFacets(new String[0]))
            .getDisabledFacets();
    this.client =
        OpenLineageClient.builder()
            .transport(new TransportFactory(openLineageYaml.getTransportConfig()).build())
            .circuitBreaker(new CircuitBreakerFactory(openLineageYaml.getCircuitBreaker()).build())
            .disableFacets(disabledFacets)
            .build();
    this.runId = UUID.randomUUID();
    this.jobName = extractJobName(conf);
    this.jobNamespace = conf.get(HiveOpenLineageConfigParser.NAMESPACE);
  }

  public void emit(OpenLineage.RunEvent event) {
    try {
      this.client.emit(event);
      log.debug(
          "Emitting lineage completed successfully: {}", OpenLineageClientUtils.toJson(event));
    } catch (OpenLineageClientException exception) {
      log.error("Could not emit lineage w/ exception", exception);
    }
  }

  // TODO: Figure out if the hook would ever be used in contexts other than when
  //  the 'hive.query.id' is available. For example, are hooks even called with Pig?
  protected String extractJobName(Configuration conf) {
    if (!conf.get("hive.query.id", "").isEmpty()) {
      // In this case, the user is running a plain Hive query directly from Hive itself.
      return "hive-query-id-" + conf.get(HiveConf.ConfVars.HIVEQUERYID.varname);
    } else if (!conf.get("pig.script.id", "").isEmpty()
        && !conf.get("pig.job.submitted.timestamp", "").isEmpty()) {
      // The user is running a Hive query from Pig. Use the job's timestamp as a pig script might
      // run multiple jobs.
      return String.format(
          "pig-%s-%s", conf.get("pig.script.id"), conf.get("pig.job.submitted.timestamp"));
    } else if (!conf.get("mapreduce.lib.hcatoutput.id", "").isEmpty()) {
      // Possibly from Pig in Tez mode in some environments (e.g. Dataproc)
      return String.format("hcat-output-%s", conf.get("mapreduce.lib.hcatoutput.id"));
    } else if (!conf.get("mapreduce.workflow.id", "").isEmpty()) {
      // Map reduce job, possibly from Pig in MR mode in some environments (e.g. Dataproc)
      return String.format("mapreduce-%s", conf.get("mapreduce.workflow.id"));
    }
    // Fall back: generate our own ID
    return String.format("custom-query-id-%s", UUID.randomUUID());
  }
}
