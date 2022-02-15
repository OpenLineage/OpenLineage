/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark3.agent.lifecycle.plan;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.PlanUtils3;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class DataSourceV2RelationVisitorTest {

  OpenLineageContext openLineageContext = mock(OpenLineageContext.class);
  DatasetFactory<OpenLineage.OutputDataset> datasetFactory = mock(DatasetFactory.class);
  DataSourceV2RelationVisitor visitor =
      new DataSourceV2RelationVisitor(openLineageContext, datasetFactory);
  LogicalPlan logicalPlan = mock(DataSourceV2Relation.class);

  @Test
  public void testApply() {
    try (MockedStatic mocked = mockStatic(PlanUtils3.class)) {
      visitor.apply(logicalPlan);
      mocked.verify(
          () ->
              PlanUtils3.fromDataSourceV2Relation(
                  datasetFactory, openLineageContext, (DataSourceV2Relation) logicalPlan),
          times(1));
    }
  }
}
