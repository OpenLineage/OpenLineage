/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.hooks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.hive.InMemoryHiveTestBase;
import io.openlineage.hive.api.OpenLineageContext;
import io.openlineage.hive.client.HiveOpenLineageConfig;
import java.net.URI;
import java.time.ZonedDateTime;
import java.util.Collections;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.junit.jupiter.api.Test;

class HiveDdlPlanTest extends InMemoryHiveTestBase {
  @Test
  void createTableHasOutputAndTask() throws Exception {
    try (Driver driver = new Driver(hiveConf)) {
      assertThat(driver.compile("CREATE TABLE ddl_plan_created (id INT)"))
          .as(driver.getErrorMsg())
          .isZero();
      QueryPlan plan = driver.getPlan();
      assertThat(plan.getOperation()).isEqualTo(HiveOperation.CREATETABLE);
      assertThat(plan.getOutputs()).isNotEmpty();
      assertThat(HiveOpenLineageHook.getValidOutputs(plan)).isNotEmpty();
      assertThat(plan.getRootTasks()).isNotEmpty();
      HookContext hook =
          new HookContext(
              plan,
              queryState,
              Collections.emptyMap(),
              null,
              null,
              null,
              null,
              null,
              null,
              false,
              null,
              null);
      hook.setHookType(HookContext.HookType.POST_EXEC_HOOK);
      assertThat(hook.getIndex()).isNotNull();
      assertThat(
              new HiveOpenLineageHook()
                  .shouldEmitEvent(
                      hook,
                      HiveOpenLineageHook.getValidInputs(plan),
                      HiveOpenLineageHook.getValidOutputs(plan)))
          .isTrue();
    }
  }

  @Test
  void createTableRequiresHiveLineageIndex() {
    try (Driver driver = new Driver(hiveConf)) {
      assertThat(driver.compile("CREATE TABLE ddl_plan_missing_index (id INT)"))
          .as(driver.getErrorMsg())
          .isZero();
      QueryPlan plan = driver.getPlan();
      HookContext hook = mock(HookContext.class);
      when(hook.getQueryPlan()).thenReturn(plan);
      when(hook.getHookType()).thenReturn(HookContext.HookType.POST_EXEC_HOOK);
      assertThat(
              new HiveOpenLineageHook()
                  .shouldEmitEvent(
                      hook,
                      HiveOpenLineageHook.getValidInputs(plan),
                      HiveOpenLineageHook.getValidOutputs(plan)))
          .isFalse();
    }
  }

  @Test
  void createTableIfNotExistsHasNoTaskWhenTableExists() throws Exception {
    createTable("ddl_plan_existing", "id;int");
    try (Driver driver = new Driver(hiveConf)) {
      assertThat(driver.compile("CREATE TABLE IF NOT EXISTS ddl_plan_existing (id INT)"))
          .as(driver.getErrorMsg())
          .isZero();
      assertThat(driver.getPlan().getRootTasks()).isEmpty();
      HookContext hook = mock(HookContext.class);
      when(hook.getQueryPlan()).thenReturn(driver.getPlan());
      when(hook.getHookType()).thenReturn(HookContext.HookType.POST_EXEC_HOOK);
      when(hook.getIndex()).thenReturn(queryState.getLineageState().getIndex());
      assertThat(
              new HiveOpenLineageHook()
                  .shouldEmitEvent(
                      hook,
                      HiveOpenLineageHook.getValidInputs(driver.getPlan()),
                      HiveOpenLineageHook.getValidOutputs(driver.getPlan())))
          .isFalse();
    }
  }

  @Test
  void createTableLikeHasCreateOperationAndOutput() throws Exception {
    createTable("ddl_plan_like_source", "id;int");
    try (Driver driver = new Driver(hiveConf)) {
      assertThat(driver.compile("CREATE TABLE ddl_plan_like_target LIKE ddl_plan_like_source"))
          .as(driver.getErrorMsg())
          .isZero();
      assertThat(driver.getPlan().getOperation()).isEqualTo(HiveOperation.CREATETABLE);
      assertThat(HiveOpenLineageHook.getValidOutputs(driver.getPlan())).isNotEmpty();
    }
  }

  @Test
  void dropTableUsesAnOutputEntity() throws Exception {
    createTable("ddl_plan_dropped", "id;int");
    try (Driver driver = new Driver(hiveConf)) {
      assertThat(driver.compile("DROP TABLE ddl_plan_dropped")).as(driver.getErrorMsg()).isZero();
      assertThat(driver.getPlan().getOperation()).isEqualTo(HiveOperation.DROPTABLE);
      assertThat(driver.getPlan().getOutputs()).isNotEmpty();
      assertThat(HiveOpenLineageHook.getValidOutputs(driver.getPlan())).isNotEmpty();
      assertThat(driver.getPlan().getOutputs())
          .extracting(WriteEntity::getTable)
          .doesNotContainNull();
    }
  }

  @Test
  void dropTableIfExistsHasNoOutputWhenTableIsMissing() {
    try (Driver driver = new Driver(hiveConf)) {
      assertThat(driver.compile("DROP TABLE IF EXISTS ddl_plan_missing"))
          .as(driver.getErrorMsg())
          .isZero();
      assertThat(driver.getPlan().getOutputs()).isEmpty();
    }
  }

  @Test
  void alterAndTruncateTablesHaveOutputEntities() throws Exception {
    createTable("ddl_plan_mutated", "id;int");
    String[] statements = {
      "ALTER TABLE ddl_plan_mutated ADD COLUMNS (extra STRING)",
      "ALTER TABLE ddl_plan_mutated RENAME TO ddl_plan_renamed",
      "TRUNCATE TABLE ddl_plan_mutated"
    };
    for (String statement : statements) {
      try (Driver driver = new Driver(hiveConf)) {
        assertThat(driver.compile(statement)).as(driver.getErrorMsg()).isZero();
        assertThat(driver.getPlan().getOutputs()).as(statement).isNotEmpty();
        assertThat(HiveOpenLineageHook.getValidOutputs(driver.getPlan()))
            .as(statement)
            .isNotEmpty();
      }
    }
  }

  @Test
  void completedAlterUsesCurrentMetastoreSchema() throws Exception {
    createTable("ddl_plan_schema", "id;int");
    Table staleTable = Hive.get(hiveConf).getTable("default", "ddl_plan_schema");
    org.apache.hadoop.hive.metastore.api.Table updatedTable = staleTable.getTTable().deepCopy();
    updatedTable.getSd().addToCols(new FieldSchema("added", "string", null));
    try (HiveMetaStoreClient client = new HiveMetaStoreClient(hiveConf)) {
      client.alter_table("default", "ddl_plan_schema", updatedTable);
    }

    QueryPlan plan = mock(QueryPlan.class);
    when(plan.getOperation()).thenReturn(HiveOperation.ALTERTABLE_ADDCOLS);
    HookContext hook = mock(HookContext.class);
    when(hook.getQueryPlan()).thenReturn(plan);
    when(hook.getConf()).thenReturn(hiveConf);
    WriteEntity output = mock(WriteEntity.class);
    when(output.getTable()).thenReturn(staleTable);
    OpenLineageContext context =
        OpenLineageContext.builder()
            .openLineage(new OpenLineage(URI.create("http://test-producer")))
            .openLineageConfig(mock(HiveOpenLineageConfig.class))
            .hookContext(hook)
            .readEntities(Collections.emptySet())
            .writeEntities(Collections.singleton(output))
            .eventTime(ZonedDateTime.now())
            .eventType(OpenLineage.RunEvent.EventType.COMPLETE)
            .build();

    assertThat(Faceting.getOutputDatasets(context, Collections.emptyList()))
        .singleElement()
        .satisfies(
            dataset ->
                assertThat(dataset.getFacets().getSchema().getFields())
                    .extracting(OpenLineage.SchemaDatasetFacetFields::getName)
                    .containsExactly("id", "added"));
  }

  @Test
  void completedRenameUsesNewMetastoreIdentifier() throws Exception {
    createTable("ddl_plan_old", "id;int");
    Table staleTable = Hive.get(hiveConf).getTable("default", "ddl_plan_old");
    try (Driver driver = new Driver(hiveConf)) {
      assertThat(driver.compile("ALTER TABLE ddl_plan_old RENAME TO ddl_plan_new"))
          .as(driver.getErrorMsg())
          .isZero();
      QueryPlan plan = driver.getPlan();
      org.apache.hadoop.hive.metastore.api.Table renamedTable = staleTable.getTTable().deepCopy();
      renamedTable.setTableName("ddl_plan_new");
      try (HiveMetaStoreClient client = new HiveMetaStoreClient(hiveConf)) {
        client.alter_table("default", "ddl_plan_old", renamedTable);
      }
      HookContext hook = mock(HookContext.class);
      when(hook.getQueryPlan()).thenReturn(plan);
      when(hook.getConf()).thenReturn(hiveConf);
      WriteEntity output = mock(WriteEntity.class);
      when(output.getTable()).thenReturn(staleTable);
      OpenLineageContext context =
          OpenLineageContext.builder()
              .openLineage(new OpenLineage(URI.create("http://test-producer")))
              .openLineageConfig(mock(HiveOpenLineageConfig.class))
              .hookContext(hook)
              .readEntities(Collections.emptySet())
              .writeEntities(Collections.singleton(output))
              .eventTime(ZonedDateTime.now())
              .eventType(OpenLineage.RunEvent.EventType.COMPLETE)
              .build();

      assertThat(Faceting.getOutputDatasets(context, Collections.emptyList()))
          .singleElement()
          .satisfies(
              dataset -> {
                assertThat(dataset.getName()).isEqualTo("default.ddl_plan_new");
                assertThat(dataset.getFacets().getSchema().getFields())
                    .extracting(OpenLineage.SchemaDatasetFacetFields::getName)
                    .containsExactly("id");
              });
    }
  }
}
