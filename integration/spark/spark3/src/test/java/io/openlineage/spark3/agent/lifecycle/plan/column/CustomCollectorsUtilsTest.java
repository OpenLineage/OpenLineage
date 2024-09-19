/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import static io.openlineage.spark3.agent.lifecycle.plan.CustomColumnLineageVisitorTestImpl.INPUT_COL_NAME;
import static io.openlineage.spark3.agent.lifecycle.plan.CustomColumnLineageVisitorTestImpl.OUTPUT_COL_NAME;
import static io.openlineage.spark3.agent.lifecycle.plan.CustomColumnLineageVisitorTestImpl.child;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.net.URI;
import java.util.Arrays;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@Slf4j
class CustomCollectorsUtilsTest {

  static LogicalPlan plan = mock(LogicalPlan.class);
  OpenLineageContext context = mock(OpenLineageContext.class);
  QueryExecution queryExecution = mock(QueryExecution.class);

  @Test
  @SneakyThrows
  void testCustomCollectorsAreApplied() {
    OpenLineage openLineage = new OpenLineage(new URI("some-url"));
    when(plan.children())
        .thenReturn(
            scala.collection.JavaConverters.collectionAsScalaIterableConverter(Arrays.asList(child))
                .asScala()
                .toSeq());
    when(context.getQueryExecution()).thenReturn(Optional.of(queryExecution));
    when(queryExecution.optimizedPlan()).thenReturn(plan);
    when(child.output()).thenReturn(ScalaConversionUtils.asScalaSeqEmpty());
    when(plan.output()).thenReturn(ScalaConversionUtils.asScalaSeqEmpty());
    when(child.children()).thenReturn(ScalaConversionUtils.asScalaSeqEmpty());
    when(context.getOpenLineage()).thenReturn(openLineage);
    when(context.getOpenLineageConfig()).thenReturn(new SparkOpenLineageConfig());

    Mockito.doCallRealMethod().when(plan).foreach(any());
    Mockito.doCallRealMethod().when(child).foreach(any());

    OpenLineage.SchemaDatasetFacet outputSchema =
        openLineage.newSchemaDatasetFacet(
            Arrays.asList(
                openLineage
                    .newSchemaDatasetFacetFieldsBuilder()
                    .name(OUTPUT_COL_NAME)
                    .type("string")
                    .build()));

    OpenLineage.ColumnLineageDatasetFacet facet =
        ColumnLevelLineageUtils.buildColumnLineageDatasetFacet(
                mock(SparkListenerEvent.class), context, outputSchema)
            .get();

    assertEquals(
        INPUT_COL_NAME,
        facet
            .getFields()
            .getAdditionalProperties()
            .get(OUTPUT_COL_NAME)
            .getInputFields()
            .get(0)
            .getField());
  }
}
