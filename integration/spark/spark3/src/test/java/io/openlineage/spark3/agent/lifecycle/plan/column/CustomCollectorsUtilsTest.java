/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.OpenLineageContext;
import java.net.URI;
import java.util.Arrays;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import scala.collection.Seq;
import scala.collection.Seq$;

@Slf4j
public class CustomCollectorsUtilsTest {

  public static final String OUTPUT_COL_NAME = "outputCol";
  public static final String INPUT_COL_NAME = "inputCol";
  public static ExprId childExprId = mock(ExprId.class);
  public static ExprId parentExprId = mock(ExprId.class);
  static LogicalPlan plan = mock(LogicalPlan.class);
  public static LogicalPlan child = mock(LogicalPlan.class);

  OpenLineageContext context = mock(OpenLineageContext.class);
  QueryExecution queryExecution = mock(QueryExecution.class);

  @Test
  @SneakyThrows
  public void testCustomCollectorsAreApplied() {
    OpenLineage openLineage = new OpenLineage(new URI("some-url"));
    when(plan.children())
        .thenReturn(
            scala.collection.JavaConverters.collectionAsScalaIterableConverter(Arrays.asList(child))
                .asScala()
                .toSeq());
    when(context.getQueryExecution()).thenReturn(Optional.of(queryExecution));
    when(queryExecution.optimizedPlan()).thenReturn(plan);
    when(child.output()).thenReturn((Seq<Attribute>) Seq$.MODULE$.empty());
    when(plan.output()).thenReturn((Seq<Attribute>) Seq$.MODULE$.empty());
    when(child.children()).thenReturn((Seq<LogicalPlan>) Seq$.MODULE$.empty());
    when(context.getOpenLineage()).thenReturn(openLineage);

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
        ColumnLevelLineageUtils.buildColumnLineageDatasetFacet(context, outputSchema).get();

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
