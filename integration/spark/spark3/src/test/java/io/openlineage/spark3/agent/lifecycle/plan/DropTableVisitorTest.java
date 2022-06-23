/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.PlanUtils3;
import java.util.List;
import java.util.Optional;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.ResolvedTable;
import org.apache.spark.sql.catalyst.plans.logical.DropTable;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import scala.collection.immutable.HashMap;
import scala.collection.immutable.Map;

public class DropTableVisitorTest {

  OpenLineageContext openLineageContext =
      OpenLineageContext.builder()
          .sparkSession(Optional.of(mock(SparkSession.class)))
          .sparkContext(mock(SparkContext.class))
          .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
          .build();

  DropTableVisitor visitor = new DropTableVisitor(openLineageContext);

  DropTable dropTable = mock(DropTable.class);
  ResolvedTable resolvedTable = mock(ResolvedTable.class);
  TableCatalog tableCatalog = mock(TableCatalog.class);
  StructType schema = new StructType();
  Table table = mock(Table.class);
  Map<String, String> tableProperties = new HashMap<>();
  DatasetIdentifier di = new DatasetIdentifier("table", "db");

  @BeforeEach
  public void setUp() {
    when(dropTable.child()).thenReturn(resolvedTable);
    when(resolvedTable.catalog()).thenReturn(tableCatalog);
    when(resolvedTable.schema()).thenReturn(schema);
    when(resolvedTable.table()).thenReturn(table);
    when(resolvedTable.identifier()).thenReturn(Identifier.of(new String[] {"db"}, "table"));
    when(table.properties())
        .thenReturn(ScalaConversionUtils.<String, String>fromMap(tableProperties));
    when(table.name()).thenReturn("db.table");
  }

  @Test
  public void testApply() {
    try (MockedStatic mocked = mockStatic(PlanUtils3.class)) {
      when(PlanUtils3.getDatasetIdentifier(
              openLineageContext,
              tableCatalog,
              Identifier.of(new String[] {"db"}, "table"),
              ScalaConversionUtils.<String, String>fromMap(tableProperties)))
          .thenReturn(Optional.of(di));

      List<OpenLineage.OutputDataset> outputDatasets = visitor.apply(dropTable);

      assertEquals(1, outputDatasets.size());
      assertEquals(
          OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.DROP,
          outputDatasets.get(0).getFacets().getLifecycleStateChange().getLifecycleStateChange());
      assertEquals("table", outputDatasets.get(0).getName());
      assertEquals("db", outputDatasets.get(0).getNamespace());
    }
  }

  @Test
  public void testApplyWhenNoIdentifierFound() {
    try (MockedStatic mocked = mockStatic(PlanUtils3.class)) {
      when(PlanUtils3.getDatasetIdentifier(
              openLineageContext,
              tableCatalog,
              Identifier.of(new String[] {"db"}, "table"),
              ScalaConversionUtils.<String, String>fromMap(tableProperties)))
          .thenReturn(Optional.empty());

      List<OpenLineage.OutputDataset> outputDatasets = visitor.apply(dropTable);

      assertEquals(0, outputDatasets.size());
    }
  }
}
