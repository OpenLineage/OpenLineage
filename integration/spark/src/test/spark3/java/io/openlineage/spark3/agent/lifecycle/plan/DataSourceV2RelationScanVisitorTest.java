package io.openlineage.spark3.agent.lifecycle.plan;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.PlanUtils3;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class DataSourceV2RelationScanVisitorTest {

  OpenLineageContext openLineageContext = mock(OpenLineageContext.class);
  DatasetFactory<OpenLineage.OutputDataset> datasetFactory = mock(DatasetFactory.class);
  DataSourceV2ScanRelationVisitor visitor =
      new DataSourceV2ScanRelationVisitor(openLineageContext, datasetFactory);
  LogicalPlan logicalPlan = mock(DataSourceV2ScanRelation.class);
  DataSourceV2Relation dataSourceV2Relation = mock(DataSourceV2Relation.class);

  @Test
  public void testApply() {
    when(((DataSourceV2ScanRelation) logicalPlan).relation()).thenReturn(dataSourceV2Relation);

    try (MockedStatic mocked = mockStatic(PlanUtils3.class)) {
      visitor.apply(logicalPlan);
      mocked.verify(
          () ->
              PlanUtils3.fromDataSourceV2Relation(
                  datasetFactory, openLineageContext, dataSourceV2Relation),
          times(1));
    }
  }
}
