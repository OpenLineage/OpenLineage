package io.openlineage.spark3.agent.lifecycle.plan;

import static org.junit.jupiter.api.Assertions.assertThrows;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.facets.TableProviderFacet;
import io.openlineage.spark.agent.lifecycle.plan.DatasetSource;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class DatasetSourceVisitorTest {

  DatasetSourceVisitor datasetSourceVisitor = new DatasetSourceVisitor();
  DataSourceV2Relation dataSourceV2Relation = Mockito.mock(DataSourceV2Relation.class);
  Table table = Mockito.mock(Table.class);
  Map<String, String> tableProperties = new HashMap<>();

  @Test
  public void testApplyExceptionIsThrownWhenNoDatasetSourceFound() {
    Exception exception =
        assertThrows(
            RuntimeException.class, () -> datasetSourceVisitor.apply(dataSourceV2Relation));

    Assert.assertTrue(exception.getMessage().startsWith("Couldn't find DatasetSource in plan"));
  }

  @Test
  public void testApplyForDefaultProvider() {
    tableProperties.put("provider", "unknown");

    table = Mockito.mock(Table.class, Mockito.withSettings().extraInterfaces(DatasetSource.class));
    Mockito.when(table.properties()).thenReturn(tableProperties);
    Mockito.when(table.name()).thenReturn("some-name");
    Mockito.when(((DatasetSource) table).namespace()).thenReturn("some-namespace");
    Mockito.when((dataSourceV2Relation).table()).thenReturn(table);
    Mockito.when(dataSourceV2Relation.schema()).thenReturn(new StructType());

    OpenLineage.Dataset dataset = datasetSourceVisitor.apply(dataSourceV2Relation).get(0);

    Assert.assertEquals("some-namespace", dataset.getNamespace());
    Assert.assertEquals("some-name", dataset.getName());
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

    OpenLineage.Dataset dataset = datasetSourceVisitor.apply(dataSourceV2Relation).get(0);

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

    Mockito.when(table.properties()).thenReturn(tableProperties);
    Mockito.when((dataSourceV2Relation).table()).thenReturn(table);
    Mockito.when(dataSourceV2Relation.schema()).thenReturn(new StructType());
    Mockito.when(table.name()).thenReturn("local.db.table");

    OpenLineage.Dataset dataset = datasetSourceVisitor.apply(dataSourceV2Relation).get(0);

    TableProviderFacet tableProviderFacet =
        (TableProviderFacet) dataset.getFacets().getAdditionalProperties().get("table_provider");

    Assert.assertEquals("file:///tmp/catalog/db/table", dataset.getNamespace());
    Assert.assertEquals("local.db.table", dataset.getName());
  }

  @Test
  public void testIsDefinedAtForDefaultProviderWhenDefined() {
    Mockito.when(dataSourceV2Relation.table())
        .thenReturn(
            Mockito.mock(Table.class, Mockito.withSettings().extraInterfaces(DatasetSource.class)));
    Assert.assertTrue(datasetSourceVisitor.isDefinedAt(dataSourceV2Relation));
  }

  @Test
  public void testIsDefinedAtForDefaultProviderWhenNotDefined() {
    Mockito.when(dataSourceV2Relation.table()).thenReturn(table);
    Assert.assertFalse(datasetSourceVisitor.isDefinedAt(dataSourceV2Relation));
  }

  @Test
  public void testIsDefinedAtForIceberg() {
    tableProperties.put("provider", "iceberg");
    Mockito.when((dataSourceV2Relation).table()).thenReturn(table);
    Mockito.when(table.properties()).thenReturn(tableProperties);
    Assert.assertTrue(datasetSourceVisitor.isDefinedAt(dataSourceV2Relation));
  }
}
