/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.shared.agent.Versions;
import io.openlineage.spark.shared.agent.util.DatasetIdentifier;
import io.openlineage.spark.shared.agent.util.ScalaConversionUtils;
import io.openlineage.spark.shared.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.CatalogUtils3;
import io.openlineage.spark3.agent.utils.PlanUtils3;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.CreateTableAsSelect;
import org.apache.spark.sql.catalyst.plans.logical.CreateV2Table;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.ReplaceTable;
import org.apache.spark.sql.catalyst.plans.logical.ReplaceTableAsSelect;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import scala.collection.immutable.HashMap;
import scala.collection.immutable.Map;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

class CreateReplaceVisitorDatasetBuilderTest {

  OpenLineageContext openLineageContext =
      OpenLineageContext.builder()
          .sparkSession(Optional.of(mock(SparkSession.class)))
          .sparkContext(mock(SparkContext.class))
          .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
          .build();

  CreateReplaceDatasetBuilder visitor = new CreateReplaceDatasetBuilder(openLineageContext);

  TableCatalog catalogTable = mock(TableCatalog.class);
  StructType schema = new StructType();
  Map<String, String> commandProperties = new HashMap<>();
  Identifier tableName = Identifier.of(new String[] {"db"}, "table");

  @Test
  public void testIsDefined() {
    assertTrue(visitor.isDefinedAtLogicalPlan(mock(CreateTableAsSelect.class)));
    assertTrue(visitor.isDefinedAtLogicalPlan(mock(ReplaceTableAsSelect.class)));
    assertTrue(visitor.isDefinedAtLogicalPlan(mock(ReplaceTable.class)));
    assertTrue(visitor.isDefinedAtLogicalPlan(mock(CreateV2Table.class)));
    assertFalse(visitor.isDefinedAtLogicalPlan(mock(LogicalPlan.class)));
  }

  @Test
  public void testApplyForCreateTableAsSelect() {
    CreateTableAsSelect logicalPlan = mock(CreateTableAsSelect.class);
    when(logicalPlan.catalog()).thenReturn(catalogTable);
    when(logicalPlan.tableName()).thenReturn(tableName);
    when(logicalPlan.tableSchema()).thenReturn(schema);
    when(logicalPlan.properties()).thenReturn(commandProperties);
    verifyApply(
        (LogicalPlan) logicalPlan,
        commandProperties,
        OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.CREATE);
  }

  @Test
  public void testApplyForReplaceTable() {
    ReplaceTable logicalPlan = mock(ReplaceTable.class);
    when(logicalPlan.catalog()).thenReturn(catalogTable);
    when(logicalPlan.tableName()).thenReturn(tableName);
    when(logicalPlan.tableSchema()).thenReturn(schema);
    when(logicalPlan.properties()).thenReturn(commandProperties);
    verifyApply(
        (LogicalPlan) logicalPlan,
        commandProperties,
        OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE);
  }

  @Test
  public void testApplyForReplaceTableAsSelect() {
    ReplaceTableAsSelect logicalPlan = mock(ReplaceTableAsSelect.class);
    when(logicalPlan.catalog()).thenReturn(catalogTable);
    when(logicalPlan.tableName()).thenReturn(tableName);
    when(logicalPlan.tableSchema()).thenReturn(schema);
    when(logicalPlan.properties()).thenReturn(commandProperties);
    verifyApply(
        (LogicalPlan) logicalPlan,
        commandProperties,
        OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE);
  }

  @Test
  public void testApplyForCreateV2Table() {
    CreateV2Table logicalPlan = mock(CreateV2Table.class);
    when(logicalPlan.catalog()).thenReturn(catalogTable);
    when(logicalPlan.tableName()).thenReturn(tableName);
    when(logicalPlan.tableSchema()).thenReturn(schema);
    when(logicalPlan.properties()).thenReturn(commandProperties);
    verifyApply(
        (LogicalPlan) logicalPlan,
        commandProperties,
        OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.CREATE);
  }

  @Test
  public void testApplyDatasetVersionIncluded() {
    ReplaceTable logicalPlan = mock(ReplaceTable.class);
    when(logicalPlan.catalog()).thenReturn(catalogTable);
    when(logicalPlan.tableName()).thenReturn(tableName);
    when(logicalPlan.tableSchema()).thenReturn(schema);
    when(logicalPlan.properties()).thenReturn(commandProperties);

    DatasetIdentifier di = new DatasetIdentifier("table", "db");
    try (MockedStatic mocked = mockStatic(PlanUtils3.class)) {
      try (MockedStatic mockedCatalog = mockStatic(CatalogUtils3.class)) {
        when(CatalogUtils3.getDatasetVersion(
                catalogTable,
                Identifier.of(new String[] {"db"}, "table"),
                ScalaConversionUtils.<String, String>fromMap(commandProperties)))
            .thenReturn(Optional.of("v2"));

        when(PlanUtils3.getDatasetIdentifier(
                openLineageContext,
                catalogTable,
                tableName,
                ScalaConversionUtils.<String, String>fromMap(commandProperties)))
            .thenReturn(Optional.of(di));

        List<OpenLineage.OutputDataset> outputDatasets = visitor.apply(logicalPlan);

        assertEquals(1, outputDatasets.size());
        assertEquals("v2", outputDatasets.get(0).getFacets().getVersion().getDatasetVersion());
      }
    }
  }

  @Test
  public void testApplyDatasetVersionMissing() {
    ReplaceTable logicalPlan = mock(ReplaceTable.class);
    when(logicalPlan.catalog()).thenReturn(catalogTable);
    when(logicalPlan.tableName()).thenReturn(tableName);
    when(logicalPlan.tableSchema()).thenReturn(schema);
    when(logicalPlan.properties()).thenReturn(commandProperties);

    DatasetIdentifier di = new DatasetIdentifier("table", "db");
    try (MockedStatic mocked = mockStatic(PlanUtils3.class)) {
      try (MockedStatic mockedCatalog = mockStatic(CatalogUtils3.class)) {
        when(CatalogUtils3.getDatasetVersion(
                catalogTable,
                Identifier.of(new String[] {"db"}, "table"),
                ScalaConversionUtils.<String, String>fromMap(commandProperties)))
            .thenReturn(Optional.empty());

        when(PlanUtils3.getDatasetIdentifier(
                openLineageContext,
                catalogTable,
                tableName,
                ScalaConversionUtils.<String, String>fromMap(commandProperties)))
            .thenReturn(Optional.of(di));

        List<OpenLineage.OutputDataset> outputDatasets = visitor.apply(logicalPlan);

        assertEquals(1, outputDatasets.size());
        assertEquals(null, outputDatasets.get(0).getFacets().getVersion());
      }
    }
  }

  private void verifyApply(
      LogicalPlan logicalPlan,
      Map<String, String> tableProperties,
      OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange lifecycleStateChange) {
    DatasetIdentifier di = new DatasetIdentifier("table", "db");
    try (MockedStatic mocked = mockStatic(PlanUtils3.class)) {
      when(PlanUtils3.getDatasetIdentifier(
              openLineageContext,
              catalogTable,
              tableName,
              ScalaConversionUtils.<String, String>fromMap(tableProperties)))
          .thenReturn(Optional.of(di));

      List<OpenLineage.OutputDataset> outputDatasets = visitor.apply(logicalPlan);

      assertEquals(1, outputDatasets.size());
      assertEquals(
          lifecycleStateChange,
          outputDatasets.get(0).getFacets().getLifecycleStateChange().getLifecycleStateChange());
      assertEquals("table", outputDatasets.get(0).getName());
      assertEquals("db", outputDatasets.get(0).getNamespace());
    }
  }

  @Test
  public void testApplyWhenNoDatasetIdentifierReturned() {
    CreateTableAsSelect logicalPlan = mock(CreateTableAsSelect.class);
    try (MockedStatic mocked = mockStatic(PlanUtils3.class)) {
      when(PlanUtils3.getDatasetIdentifier(
              openLineageContext,
              catalogTable,
              tableName,
              ScalaConversionUtils.<String, String>fromMap(logicalPlan.properties())))
          .thenReturn(Optional.empty());

      List<OpenLineage.OutputDataset> outputDatasets = visitor.apply(logicalPlan);
      assertEquals(0, outputDatasets.size());
    }
  }
}
