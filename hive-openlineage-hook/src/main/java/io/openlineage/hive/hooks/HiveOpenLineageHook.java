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
package io.openlineage.hive.hooks;

import io.openlineage.client.OpenLineage;
import io.openlineage.hive.api.OpenLineageContext;
import io.openlineage.hive.client.EventEmitter;
import io.openlineage.hive.client.HiveOpenLineageConfigParser;
import io.openlineage.hive.client.Versions;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;

public class HiveOpenLineageHook implements ExecuteWithHookContext {

  private static final Set<HiveOperation> SUPPORTED_OPERATIONS = new HashSet<>();

  static {
    SUPPORTED_OPERATIONS.add(HiveOperation.QUERY);
    SUPPORTED_OPERATIONS.add(HiveOperation.CREATETABLE_AS_SELECT);
  }

  public static Set<ReadEntity> getValidInputs(QueryPlan queryPlan) {
    Set<ReadEntity> validInputs = new HashSet<>();
    for (ReadEntity readEntity : queryPlan.getInputs()) {
      Entity.Type entityType = readEntity.getType();
      if ((entityType == Entity.Type.TABLE || entityType == Entity.Type.PARTITION)
          && !readEntity.isDummy()) {
        validInputs.add(readEntity);
      }
    }
    return validInputs;
  }

  public static Set<WriteEntity> getValidOutputs(QueryPlan queryPlan) {
    Set<WriteEntity> validOutputs = new HashSet<>();
    for (WriteEntity writeEntity : queryPlan.getOutputs()) {
      Entity.Type entityType = writeEntity.getType();
      if ((entityType == Entity.Type.TABLE || entityType == Entity.Type.PARTITION)
          && !writeEntity.isDummy()) {
        validOutputs.add(writeEntity);
      }
    }
    return validOutputs;
  }

  @Override
  public void run(HookContext hookContext) throws Exception {
    QueryPlan queryPlan = hookContext.getQueryPlan();
    Set<ReadEntity> validInputs = getValidInputs(queryPlan);
    Set<WriteEntity> validOutputs = getValidOutputs(queryPlan);
    if (hookContext.getHookType() != HookContext.HookType.POST_EXEC_HOOK
        || SessionState.get() == null
        || hookContext.getIndex() == null
        || !SUPPORTED_OPERATIONS.contains(queryPlan.getOperation())
        || queryPlan.isExplain()
        || queryPlan.getInputs().isEmpty()
        || queryPlan.getOutputs().isEmpty()
        || validInputs.isEmpty()
        || validOutputs.isEmpty()) {
      return;
    }
    OpenLineageContext olContext =
        OpenLineageContext.builder()
            .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
            .queryString(hookContext.getQueryPlan().getQueryString())
            .eventTime(Instant.ofEpochMilli(System.currentTimeMillis()).atZone(ZoneId.of("UTC")))
            .readEntities(validInputs)
            .writeEntities(validOutputs)
            .hadoopConf(hookContext.getConf())
            .openlineageHiveIntegrationVersion(Versions.getVersion())
            .openLineageConfig(
                HiveOpenLineageConfigParser.extractFromHadoopConf(hookContext.getConf()))
            .build();
    try (EventEmitter emitter = new EventEmitter(olContext)) {
      OpenLineage.RunEvent runEvent = Faceting.getRunEvent(emitter, olContext);
      emitter.emit(runEvent);
    }
  }
}
