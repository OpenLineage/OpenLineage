package io.openlineage.spark3.agent.lifecycle.plan;

import static org.junit.jupiter.api.Assertions.assertThrows;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.facets.TableProviderFacet;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class DataSourceV2RelationVisitorTest {

  DataSourceV2RelationVisitor dataSourceV2RelationVisitor = new DataSourceV2RelationVisitor();
  DataSourceV2Relation dataSourceV2Relation = Mockito.mock(DataSourceV2Relation.class);
  Table table = Mockito.mock(Table.class);
  Map<String, String> tableProperties = new HashMap<>();

  @Test
  public void testApplyExceptionIsThrownWhenNonSupportedProvider() {
    Exception exception =
        assertThrows(
            RuntimeException.class, () -> dataSourceV2RelationVisitor.apply(dataSourceV2Relation));

    Assert.assertTrue(
        exception.getMessage().startsWith("Couldn't find provider for dataset in plan"));
  }

  @Test
  public void testApplyForIcebergOnGS() {
    tableProperties.put("provider", "iceberg");
    tableProperties.put("format", "iceberg/parquet");
    tableProperties.put("location", "gs://bucket/catalog/db/table");

    Mockito.when(table.properties()).thenReturn(tableProperties);
    Mockito.when((dataSourceV2Relation).table()).thenReturn(table);
    Mockito.when(dataSourceV2Relation.schema()).thenReturn(new StructType());
    Mockito.when(table.name()).thenReturn("remote-gcs.db.table");

    OpenLineage.Dataset dataset = dataSourceV2RelationVisitor.apply(dataSourceV2Relation).get(0);

    TableProviderFacet tableProviderFacet =
        (TableProviderFacet) dataset.getFacets().getAdditionalProperties().get("table_provider");

    Assert.assertEquals("parquet", tableProviderFacet.getFormat());
    Assert.assertEquals("iceberg", tableProviderFacet.getProvider());
    Assert.assertEquals("gs://bucket/catalog/db/table", dataset.getNamespace());
    Assert.assertEquals("remote-gcs.db.table", dataset.getName());
  }

  @Test
  public void testApplyForIcebergOnLocal() {
    tableProperties.put("provider", "iceberg");
    tableProperties.put("location", "/tmp/catalog/db/table");
    tableProperties.put("format", "iceberg/parquet");

    Mockito.when(table.properties()).thenReturn(tableProperties);
    Mockito.when((dataSourceV2Relation).table()).thenReturn(table);
    Mockito.when(dataSourceV2Relation.schema()).thenReturn(new StructType());
    Mockito.when(table.name()).thenReturn("local.db.table");

    OpenLineage.Dataset dataset = dataSourceV2RelationVisitor.apply(dataSourceV2Relation).get(0);

    TableProviderFacet tableProviderFacet =
        (TableProviderFacet) dataset.getFacets().getAdditionalProperties().get("table_provider");

    Assert.assertEquals("file:///tmp/catalog/db/table", dataset.getNamespace());
    Assert.assertEquals("local.db.table", dataset.getName());
  }

  @Test
  public void testIsDefinedAtForNonDefinedProvider() {
    Mockito.when(dataSourceV2Relation.table()).thenReturn(table);
    Assert.assertFalse(dataSourceV2RelationVisitor.isDefinedAt(dataSourceV2Relation));
  }

  @Test
  public void testIsDefinedAtForIceberg() {
    tableProperties.put("provider", "iceberg");
    Mockito.when((dataSourceV2Relation).table()).thenReturn(table);
    Mockito.when(table.properties()).thenReturn(tableProperties);
    Assert.assertTrue(dataSourceV2RelationVisitor.isDefinedAt(dataSourceV2Relation));
  }

  @Test
  public void testIsDefinedForDelta() {
    tableProperties.put("provider", "delta");
    Mockito.when((dataSourceV2Relation).table()).thenReturn(table);
    Mockito.when(table.properties()).thenReturn(tableProperties);
    Assert.assertTrue(dataSourceV2RelationVisitor.isDefinedAt(dataSourceV2Relation));
  }

  @Test
  public void testApplyDeltaLocal() {
    tableProperties.put("provider", "delta");
    tableProperties.put("location", "file:/tmp/delta/spark-warehouse/tbl");
    tableProperties.put("format", "parquet");

    Mockito.when(table.properties()).thenReturn(tableProperties);
    Mockito.when((dataSourceV2Relation).table()).thenReturn(table);
    Mockito.when(dataSourceV2Relation.schema()).thenReturn(new StructType());
    Mockito.when(table.name()).thenReturn("table");

    OpenLineage.Dataset dataset = dataSourceV2RelationVisitor.apply(dataSourceV2Relation).get(0);

    Assert.assertEquals("file:/tmp/delta/spark-warehouse/tbl", dataset.getNamespace());
    Assert.assertEquals("table", dataset.getName());
  }
}
