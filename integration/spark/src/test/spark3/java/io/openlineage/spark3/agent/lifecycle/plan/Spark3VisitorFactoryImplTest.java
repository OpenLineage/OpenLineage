package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.plan.DatasetSource;
import java.util.List;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class Spark3VisitorFactoryImplTest {

  DatasetSourceVisitor datasetSourceVisitor = new DatasetSourceVisitor();

  @Test
  public void findDataSourceReturnsRelationTable() {
    Table table =
        Mockito.mock(Table.class, Mockito.withSettings().extraInterfaces(DatasetSource.class));
    Mockito.when(table.name()).thenReturn("some-table");
    Mockito.when(((DatasetSource) table).namespace()).thenReturn("some-namespace");

    DataSourceV2Relation dataSourceV2Relation = Mockito.mock(DataSourceV2Relation.class);
    Mockito.when(dataSourceV2Relation.table()).thenReturn(table);
    Mockito.when(dataSourceV2Relation.schema()).thenReturn(new StructType());

    List<OpenLineage.Dataset> datasets = datasetSourceVisitor.apply(dataSourceV2Relation);

    Assert.assertEquals(1, datasets.size());
    Assert.assertEquals("some-table", datasets.get(0).getName());
  }
}
