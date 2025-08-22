/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.hooks;

import static org.apache.hadoop.hive.ql.hooks.HookContext.HookType;

import io.openlineage.client.OpenLineage;
import io.openlineage.hive.api.OpenLineageContext;
import io.openlineage.hive.client.EventEmitter;
import io.openlineage.hive.client.HiveOpenLineageConfigParser;
import io.openlineage.hive.client.Versions;
import java.lang.reflect.Field;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashSet;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.session.HiveSession;
import org.apache.hive.service.cli.session.HiveSessionHook;
import org.apache.hive.service.cli.session.HiveSessionHookContext;
import org.apache.hive.service.cli.session.HiveSessionHookContextImpl;

@Slf4j
public class HiveOpenLineageHook implements ExecuteWithHookContext, HiveSessionHook {

  private static final Set<HiveOperation> SUPPORTED_OPERATIONS = new HashSet<>();
  private static final Set<HookContext.HookType> SUPPORTED_HOOK_TYPES = new HashSet<>();

  static {
    SUPPORTED_OPERATIONS.add(HiveOperation.QUERY);
    SUPPORTED_OPERATIONS.add(HiveOperation.CREATETABLE_AS_SELECT);
    SUPPORTED_HOOK_TYPES.add(HookType.POST_EXEC_HOOK);
    SUPPORTED_HOOK_TYPES.add(HookType.ON_FAILURE_HOOK);
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

  // This method exists only to record session creation timestamp.
  // See https://github.com/OpenLineage/OpenLineage/issues/3784
  @Override
  public void run(HiveSessionHookContext sessionHookContext) throws HiveSQLException {
    try {
      HiveConf conf = sessionHookContext.getSessionConf();
      if (sessionHookContext instanceof HiveSessionHookContextImpl) {
        HiveSessionHookContextImpl hiveSessionHookContext =
            (HiveSessionHookContextImpl) sessionHookContext;
        Field f = hiveSessionHookContext.getClass().getDeclaredField("hiveSession");
        f.setAccessible(true);
        HiveSession hiveSession = (HiveSession) f.get(hiveSessionHookContext);
        conf.setLong("hive.session.creationTimestamp", hiveSession.getCreationTime());
      }
    } catch (Exception e) {
      log.error("Error occurred while recording session creation timestamp:", e);
    }
  }

  @Override
  public void run(HookContext hookContext) throws Exception {
    try {
      QueryPlan queryPlan = hookContext.getQueryPlan();
      Set<ReadEntity> validInputs = getValidInputs(queryPlan);
      Set<WriteEntity> validOutputs = getValidOutputs(queryPlan);
      if (!SUPPORTED_HOOK_TYPES.contains(hookContext.getHookType())
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
      OpenLineage.RunEvent.EventType eventType;
      if (hookContext.getHookType() == HookType.POST_EXEC_HOOK) {
        // It is a successful query
        eventType = OpenLineage.RunEvent.EventType.COMPLETE;
      } else { // HookType.ON_FAILURE_HOOK
        // It is a failed query
        eventType = OpenLineage.RunEvent.EventType.FAIL;
      }
      OpenLineageContext olContext =
          OpenLineageContext.builder()
              .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
              .openLineageConfig(
                  HiveOpenLineageConfigParser.extractFromHadoopConf(hookContext.getConf()))
              .hookContext(hookContext)
              .eventTime(ZonedDateTime.now(ZoneOffset.UTC))
              .eventType(eventType)
              .readEntities(validInputs)
              .writeEntities(validOutputs)
              .build();
      try (EventEmitter emitter = new EventEmitter(olContext)) {
        OpenLineage.RunEvent runEvent = Faceting.getRunEvent(emitter, olContext);
        emitter.emit(runEvent);
      }
    } catch (Exception e) {
      // Don't let the query fail. Just log the error.
      log.error("Error occurred during lineage creation:", e);
    }
  }
}
