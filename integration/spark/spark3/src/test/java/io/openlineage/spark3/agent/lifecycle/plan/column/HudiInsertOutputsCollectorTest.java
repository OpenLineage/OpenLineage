/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageContext;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.hudi.command.InsertIntoHoodieTableCommand;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Option;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class HudiInsertOutputsCollectorTest {

  private static final OpenLineage OPEN_LINEAGE =
      new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);

  ColumnLevelLineageBuilder builder = mock(ColumnLevelLineageBuilder.class);
  ColumnLevelLineageContext context = mock(ColumnLevelLineageContext.class);
  InsertIntoHoodieTableCommand command = mock(InsertIntoHoodieTableCommand.class);
  LogicalPlan query = mock(LogicalPlan.class);

  @BeforeEach
  void setup() {
    when(context.getBuilder()).thenReturn(builder);
    when(command.partitionSpec()).thenReturn(ScalaConversionUtils.asScalaMapEmpty());
  }

  @Test
  void testUnaliasedQueryIsAlignedByPosition() {
    // INSERT INTO t SELECT id, name, ts FROM s, into a table whose first column is entity_id
    List<Attribute> outputs = queryOutputs("id", "name", "ts");

    boolean collected =
        HudiInsertOutputsCollector.collectByPosition(
            context,
            command,
            query,
            schema(
                "_hoodie_commit_time",
                "_hoodie_commit_seqno",
                "_hoodie_record_key",
                "_hoodie_partition_path",
                "_hoodie_file_name",
                "entity_id",
                "name",
                "ts"));

    assertThat(collected).isTrue();
    verify(builder).addOutput(outputs.get(0).exprId(), "entity_id");
    verify(builder).addOutput(outputs.get(1).exprId(), "name");
    verify(builder).addOutput(outputs.get(2).exprId(), "ts");
  }

  @Test
  void testValuesAreAlignedByPosition() {
    // INSERT INTO t VALUES (1, 'a', 10): Spark names the columns col1, col2, col3
    List<Attribute> outputs = queryOutputs("col1", "col2", "col3");

    assertThat(
            HudiInsertOutputsCollector.collectByPosition(
                context, command, query, schema("entity_id", "name", "ts")))
        .isTrue();
    verify(builder).addOutput(outputs.get(0).exprId(), "entity_id");
    verify(builder).addOutput(outputs.get(2).exprId(), "ts");
  }

  @Test
  void testStaticPartitionColumnsAreSkipped() {
    // INSERT INTO t PARTITION (dt = '2026-11-16') SELECT id, name FROM s
    Map<String, Option<String>> spec = new HashMap<>();
    spec.put("dt", Option.apply("2026-11-16"));
    when(command.partitionSpec()).thenReturn(ScalaConversionUtils.fromJavaMap(spec));
    List<Attribute> outputs = queryOutputs("id", "label");

    assertThat(
            HudiInsertOutputsCollector.collectByPosition(
                context, command, query, schema("entity_id", "name", "dt")))
        .isTrue();
    verify(builder).addOutput(outputs.get(0).exprId(), "entity_id");
    verify(builder).addOutput(outputs.get(1).exprId(), "name");
    verify(builder, never()).addOutput(any(), eq("dt"));
  }

  @Test
  void testDynamicPartitionColumnsAreAligned() {
    Map<String, Option<String>> spec = new HashMap<>();
    spec.put("dt", Option.empty());
    when(command.partitionSpec()).thenReturn(ScalaConversionUtils.fromJavaMap(spec));
    List<Attribute> outputs = queryOutputs("id", "day");

    assertThat(
            HudiInsertOutputsCollector.collectByPosition(
                context, command, query, schema("entity_id", "dt")))
        .isTrue();
    verify(builder).addOutput(outputs.get(1).exprId(), "dt");
  }

  @Test
  void testQueryMatchingByNameIsLeftToUsualCollection() {
    // Hudi aligns by name when every column has a match, in any order or case
    queryOutputs("TS", "entity_id", "name");

    assertThat(
            HudiInsertOutputsCollector.collectByPosition(
                context, command, query, schema("entity_id", "name", "ts")))
        .isFalse();
    verify(builder, never()).addOutput(any(), anyString());
  }

  @Test
  void testDifferentColumnCountIsLeftToUsualCollection() {
    queryOutputs("id", "name");

    assertThat(
            HudiInsertOutputsCollector.collectByPosition(
                context, command, query, schema("entity_id", "name", "ts")))
        .isFalse();
  }

  @Test
  void testOtherPlansAreLeftToUsualCollection() {
    queryOutputs("id");
    LogicalPlan plan = mock(LogicalPlan.class);

    assertThat(
            HudiInsertOutputsCollector.collectByPosition(context, plan, query, schema("entity_id")))
        .isFalse();
  }

  private List<Attribute> queryOutputs(String... names) {
    List<Attribute> attributes =
        Arrays.stream(names)
            .map(
                name -> {
                  Attribute attribute = mock(Attribute.class);
                  when(attribute.name()).thenReturn(name);
                  when(attribute.exprId()).thenReturn(mock(ExprId.class));
                  return attribute;
                })
            .collect(Collectors.toList());
    when(query.output()).thenReturn(ScalaConversionUtils.fromList(attributes).toSeq());
    return attributes;
  }

  private static OpenLineage.SchemaDatasetFacet schema(String... names) {
    return OPEN_LINEAGE
        .newSchemaDatasetFacetBuilder()
        .fields(
            Arrays.stream(names)
                .map(name -> OPEN_LINEAGE.newSchemaDatasetFacetFieldsBuilder().name(name).build())
                .collect(Collectors.toList()))
        .build();
  }
}
