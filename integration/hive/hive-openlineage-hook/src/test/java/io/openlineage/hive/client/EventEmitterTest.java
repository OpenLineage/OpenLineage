/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.hive.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.hive.api.OpenLineageContext;
import java.net.URI;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.UUID;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryInfo;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.junit.jupiter.api.Test;

class EventEmitterTest {
  @Test
  void testRunIdFromQueryInfo() {
    HookContext hookContext = mock(HookContext.class);
    QueryInfo queryInfo = mock(QueryInfo.class);
    when(queryInfo.getBeginTime()).thenReturn(1_700_000_000_000L);
    when(queryInfo.getOperationId()).thenReturn("operation-id");
    when(hookContext.getQueryInfo()).thenReturn(queryInfo);

    UUID runId = EventEmitter.getRunId(context(hookContext));

    assertThat(runId).isEqualTo(EventEmitter.getRunId(context(hookContext)));
    assertThat(runId.version()).isEqualTo(7);
  }

  @Test
  void testRunIdFromQueryPlanWhenQueryInfoIsMissing() {
    // Hive CLI does not populate QueryInfo
    HookContext hookContext = mock(HookContext.class);
    QueryPlan queryPlan = mock(QueryPlan.class);
    when(queryPlan.getQueryId()).thenReturn("hive_20260101000000_query-id");
    when(queryPlan.getQueryStartTime()).thenReturn(1_700_000_000_000L);
    when(hookContext.getQueryInfo()).thenReturn(null);
    when(hookContext.getQueryPlan()).thenReturn(queryPlan);

    UUID runId = EventEmitter.getRunId(context(hookContext));

    assertThat(runId).isEqualTo(EventEmitter.getRunId(context(hookContext)));
    assertThat(runId.version()).isEqualTo(7);
  }

  @Test
  void testRandomRunIdWhenNoQueryDataAvailable() {
    HookContext hookContext = mock(HookContext.class);

    assertThat(EventEmitter.getRunId(context(hookContext))).isNotNull();
  }

  private OpenLineageContext context(HookContext hookContext) {
    return OpenLineageContext.builder()
        .openLineage(new OpenLineage(URI.create("http://test-producer")))
        .openLineageConfig(new HiveOpenLineageConfig())
        .hookContext(hookContext)
        .readEntities(Collections.emptySet())
        .writeEntities(Collections.emptySet())
        .eventTime(ZonedDateTime.now())
        .eventType(OpenLineage.RunEvent.EventType.START)
        .build();
  }

  @Test
  void usesHiveJobNamespaceAndPreservesExplicitConfiguration() throws Exception {
    HiveConf conf = new HiveConf();
    HookContext hook = mock(HookContext.class);
    QueryInfo query = mock(QueryInfo.class);
    when(query.getBeginTime()).thenReturn(1L);
    when(query.getOperationId()).thenReturn("orders");
    when(hook.getQueryInfo()).thenReturn(query);
    when(hook.getConf()).thenReturn(conf);
    when(hook.getOperationName()).thenReturn("INSERT");
    HiveOpenLineageConfig config = new HiveOpenLineageConfig();
    config.setTransportConfig(new io.openlineage.client.transports.ConsoleConfig());
    OpenLineageContext context =
        OpenLineageContext.builder()
            .openLineage(new OpenLineage(URI.create("https://openlineage.io/test")))
            .openLineageConfig(config)
            .hookContext(hook)
            .readEntities(Collections.emptySet())
            .writeEntities(Collections.emptySet())
            .eventTime(ZonedDateTime.now())
            .eventType(OpenLineage.RunEvent.EventType.START)
            .build();
    try (EventEmitter emitter = new EventEmitter(context)) {
      assertThat(emitter.getJobNamespace()).isEqualTo("hive-jobs");
    }
    for (String namespace : new String[] {"custom-hive", "default"}) {
      conf.set(HiveOpenLineageConfigParser.NAMESPACE_KEY, namespace);
      try (EventEmitter emitter = new EventEmitter(context)) {
        assertThat(emitter.getJobNamespace()).isEqualTo(namespace);
      }
    }
  }
}
