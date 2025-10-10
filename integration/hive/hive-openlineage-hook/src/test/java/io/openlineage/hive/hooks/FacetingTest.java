/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.hooks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.SchemaDatasetFacet;
import io.openlineage.client.OpenLineage.SymlinksDatasetFacet;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.hive.api.OpenLineageContext;
import io.openlineage.hive.client.HiveOpenLineageConfig;
import io.openlineage.hive.facets.HivePropertiesFacet;
import io.openlineage.hive.facets.HiveQueryInfoFacet;
import io.openlineage.hive.facets.HiveSessionInfoFacet;
import java.net.URI;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.UUID;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class FacetingTest {

  @Mock private HookContext hookContext;
  @Mock private HiveConf configuration;
  @Mock private HiveOpenLineageConfig openLineageConfig;

  private OpenLineage openLineage;
  private ZonedDateTime testEventTime;

  @BeforeEach
  void setUp() {
    openLineage = new OpenLineage(URI.create("http://test-producer"));
    testEventTime = ZonedDateTime.now();
  }

  @Test
  void testGetProcessingEngineFacet() {
    OpenLineageContext context = createMinimalContext(hookContext);

    OpenLineage.ProcessingEngineRunFacet result = Faceting.getProcessingEngineFacet(context);

    assertThat(result).isNotNull();
    assertThat(result.getName()).isEqualTo("hive");
    assertThat(result.getVersion()).isNotNull();
    assertThat(result.getOpenlineageAdapterVersion()).isNotNull();
  }

  @Test
  void testGetHivePropertiesFacet() {
    when(configuration.get("hive.openlineage.capturedProperties")).thenReturn(null);
    when(hookContext.getConf()).thenReturn(configuration);
    when(configuration.iterator()).thenReturn(Collections.emptyIterator());
    OpenLineageContext context = createMinimalContext(hookContext);

    HivePropertiesFacet result = Faceting.getHivePropertiesFacet(context);

    assertThat(result).isNotNull();
    assertThat(result.getProperties()).isNotNull();
  }

  @Test
  void testGetHiveQueryInfoFacet() {
    QueryState queryState = mock(QueryState.class);
    when(queryState.getQueryId()).thenReturn("test-query-id");
    when(hookContext.getQueryState()).thenReturn(queryState);
    when(hookContext.getOperationName()).thenReturn("SELECT");
    OpenLineageContext context = createMinimalContext(hookContext);

    HiveQueryInfoFacet result = Faceting.getHiveQueryInfoFacet(context);

    assertThat(result).isNotNull();
    assertThat(result.getQueryId()).isEqualTo("test-query-id");
    assertThat(result.getOperationName()).isEqualTo("SELECT");
  }

  @Test
  void testGetHiveSessionInfoFacet() {
    UserGroupInformation ugi = mock(UserGroupInformation.class);
    lenient().when(ugi.getUserName()).thenReturn("user");
    when(hookContext.getUserName()).thenReturn("user");
    when(hookContext.getUgi()).thenReturn(ugi);
    when(hookContext.getConf()).thenReturn(configuration);
    when(hookContext.getSessionId()).thenReturn("test-session-id");
    when(hookContext.getIpAddress()).thenReturn("127.0.0.1");
    OpenLineageContext context = createMinimalContext(hookContext);

    HiveSessionInfoFacet result = Faceting.getHiveSessionInfoFacet(context);

    assertThat(result).isNotNull();
    assertThat(result.getUsername()).isEqualTo("user");
    assertThat(result.getClientIp()).isEqualTo("127.0.0.1");
    assertThat(result.getSessionId()).isEqualTo("test-session-id");
  }

  @Test
  void testGetHiveSessionInfoFacetWithCreationTimestamp() {
    long creationTime = System.currentTimeMillis();
    UserGroupInformation ugi = mock(UserGroupInformation.class);
    lenient().when(ugi.getUserName()).thenReturn("user");

    when(configuration.getLong("hive.session.creationTimestamp", 0)).thenReturn(creationTime);
    when(hookContext.getUserName()).thenReturn("user");
    when(hookContext.getUgi()).thenReturn(ugi);
    when(hookContext.getConf()).thenReturn(configuration);
    OpenLineageContext context = createMinimalContext(hookContext);

    HiveSessionInfoFacet result = Faceting.getHiveSessionInfoFacet(context);

    assertThat(result).isNotNull();
    assertThat(result.getCreationTime()).isNotNull();
    assertThat(result.getCreationTime().toInstant().toEpochMilli()).isEqualTo(creationTime);
  }

  @Test
  void testGetParentRunFacetWithValidParent() {
    UUID parentRunId = UUID.randomUUID();
    when(openLineageConfig.getParentRunId()).thenReturn(parentRunId.toString());
    when(openLineageConfig.getParentJobName()).thenReturn("parent-job");
    when(openLineageConfig.getParentJobNamespace()).thenReturn("parent-namespace");
    OpenLineageContext context = createMinimalContext(hookContext);

    OpenLineage.ParentRunFacet result = Faceting.getParentRunFacet(context);

    assertThat(result).isNotNull();
    assertThat(result.getRun().getRunId()).isEqualTo(parentRunId);
    assertThat(result.getJob().getNamespace()).isEqualTo("parent-namespace");
  }

  @Test
  void testGetParentRunFacetWithInvalidParent() {
    when(openLineageConfig.getParentRunId()).thenReturn("invalid-uuid");
    OpenLineageContext context = createMinimalContext(hookContext);

    OpenLineage.ParentRunFacet result = Faceting.getParentRunFacet(context);

    assertThat(result).isNull();
  }

  @Test
  void testGetParentRunFacetWithMissingJobInfo() {
    UUID parentRunId = UUID.randomUUID();
    when(openLineageConfig.getParentRunId()).thenReturn(parentRunId.toString());
    when(openLineageConfig.getParentJobName()).thenReturn(null);
    OpenLineageContext context = createMinimalContext(hookContext);

    OpenLineage.ParentRunFacet result = Faceting.getParentRunFacet(context);

    assertThat(result).isNull();
  }

  @Test
  void testGetSQLJobFacet() {
    QueryPlan queryPlan = mock(QueryPlan.class);
    when(queryPlan.getQueryString()).thenReturn("SELECT * FROM test_table");
    when(hookContext.getQueryPlan()).thenReturn(queryPlan);
    OpenLineageContext context = createMinimalContext(hookContext);

    OpenLineage.SQLJobFacet result = Faceting.getSQLJobFacet(context);

    assertThat(result).isNotNull();
    assertThat(result.getQuery()).isEqualTo("SELECT * FROM test_table");
    assertThat(result.getDialect()).isEqualTo("hive");
  }

  @Test
  void testGetJobTypeFacet() {
    OpenLineageContext context = createMinimalContext(hookContext);

    OpenLineage.JobTypeJobFacet result = Faceting.getJobTypeFacet(context);

    assertThat(result).isNotNull();
    assertThat(result.getJobType()).isEqualTo("QUERY");
    assertThat(result.getProcessingType()).isEqualTo("BATCH");
    assertThat(result.getIntegration()).isEqualTo("HIVE");
  }

  @Test
  void testGetSchemaDatasetFacet() {
    Table table = mock(Table.class);
    when(table.getCols())
        .thenReturn(Collections.singletonList(new FieldSchema("col1", "string", "First column")));

    SchemaDatasetFacet result = Faceting.getSchemaDatasetFacet(openLineage, table);

    assertThat(result).isNotNull();
    assertThat(result.getFields()).isNotEmpty();
    assertThat(result.getFields().get(0).getName()).isEqualTo("col1");
    assertThat(result.getFields().get(0).getType()).isEqualTo("string");
  }

  @Test
  void testGetSymlinkFacetsWithNoSymlinks() {
    DatasetIdentifier di = new DatasetIdentifier("test_table", "default");

    SymlinksDatasetFacet result = Faceting.getSymlinkFacets(openLineage, di);

    assertThat(result).isNull();
  }

  // cannot mock static methods with Mockito, so using real object here
  private OpenLineageContext createMinimalContext(HookContext hookContext) {
    return OpenLineageContext.builder()
        .openLineage(openLineage)
        .openLineageConfig(openLineageConfig)
        .hookContext(hookContext)
        .readEntities(Collections.emptySet())
        .writeEntities(Collections.emptySet())
        .eventTime(testEventTime)
        .eventType(OpenLineage.RunEvent.EventType.START)
        .build();
  }
}
