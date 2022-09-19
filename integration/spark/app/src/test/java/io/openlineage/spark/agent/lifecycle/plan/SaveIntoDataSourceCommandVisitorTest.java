/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import static io.openlineage.client.OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation;
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand;
import org.apache.spark.sql.sources.CreatableRelationProvider;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.StringType$;
import org.junit.jupiter.api.Test;
import scala.Predef;
import scala.Tuple2;
import scala.collection.Map;

class SaveIntoDataSourceCommandVisitorTest {

  SaveIntoDataSourceCommand command;
  OpenLineageContext context = mock(OpenLineageContext.class);
  SaveIntoDataSourceCommandVisitor visitor = new SaveIntoDataSourceCommandVisitor(context);
  DatasetFactory datasetFactory = mock(DatasetFactory.class);
  OpenLineage.OutputDataset dataset = mock(OpenLineage.OutputDataset.class);

  @Test
  void testSchemaExtractedFromLocalRelation() {
    when(context.getOpenLineage()).thenReturn(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI));

    Attribute attr1 = mock(Attribute.class);
    when(attr1.dataType()).thenReturn(StringType$.MODULE$);
    when(attr1.name()).thenReturn("a");

    Attribute attr2 = mock(Attribute.class);
    when(attr2.dataType()).thenReturn(IntegerType$.MODULE$);
    when(attr2.name()).thenReturn("b");

    Map<String, String> options =
        scala.collection.JavaConversions$.MODULE$.mapAsScalaMap(
            Collections.singletonMap("path", "some-path"));

    command = mock(SaveIntoDataSourceCommand.class);
    DeltaDataSource deltaDataSource = mock(DeltaDataSource.class);
    LocalRelation localRelation = mock(LocalRelation.class);
    when(localRelation.output())
        .thenReturn(Arrays.asList(attr1, attr2).stream().collect(ScalaConversionUtils.toSeq()));
    when(command.dataSource()).thenReturn(deltaDataSource);
    when(command.schema()).thenReturn(null);
    when(command.mode()).thenReturn(SaveMode.Overwrite);
    when(command.query()).thenReturn(localRelation);
    when(command.options()).thenReturn(options.toMap(Predef.<Tuple2<String, String>>conforms()));
    when(localRelation.output())
        .thenReturn(Arrays.asList(attr1, attr2).stream().collect(ScalaConversionUtils.toSeq()));
    when(datasetFactory.getDataset(any(), any(), eq(OVERWRITE))).thenReturn(dataset);

    List<OpenLineage.OutputDataset> datasets =
        visitor.apply(mock(SparkListenerEvent.class), command);

    assertEquals(1, datasets.size());
    assertEquals(
        OVERWRITE, datasets.get(0).getFacets().getLifecycleStateChange().getLifecycleStateChange());
    assertEquals("a", datasets.get(0).getFacets().getSchema().getFields().get(0).getName());
    assertEquals("string", datasets.get(0).getFacets().getSchema().getFields().get(0).getType());
    assertEquals("b", datasets.get(0).getFacets().getSchema().getFields().get(1).getName());
    assertEquals("integer", datasets.get(0).getFacets().getSchema().getFields().get(1).getType());
  }

  abstract class DeltaDataSource implements CreatableRelationProvider {}
}
