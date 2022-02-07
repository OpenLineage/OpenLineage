/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark3.agent.lifecycle.plan;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation;
import org.junit.jupiter.api.Test;

public class DataSourceV2RelationVisitorTest {

  private static final String SOME_VERSION = "version_1";
  OpenLineageContext openLineageContext = mock(OpenLineageContext.class);
  DatasetFactory<OpenLineage.OutputDataset> datasetFactory = mock(DatasetFactory.class);
  OpenLineage.DatasetVersionDatasetFacet facet = mock(OpenLineage.DatasetVersionDatasetFacet.class);
  OpenLineage openLineage = mock(OpenLineage.class);
  DataSourceV2RelationVisitor visitor =
      new DataSourceV2RelationVisitor(openLineageContext, datasetFactory, false);

  @Test
  void testIsDefined() {
    assertTrue(visitor.isDefinedAt(mock(DataSourceV2Relation.class)));
    assertTrue(visitor.isDefinedAt(mock(DataSourceV2ScanRelation.class)));
    assertFalse(visitor.isDefinedAt(mock(LogicalPlan.class)));
  }

  @Test
  void testApplyV2Relation() {
    // FIXME: TEST LATER
    //    DataSourceV2Relation relation = mock(DataSourceV2Relation.class);
    //    DataSourceV2ScanRelation scanRelation = mock(DataSourceV2ScanRelation.class);
    //    when(scanRelation.relation()).thenReturn(relation);
    //
    //    try (MockedStatic planUtils3MockedStatic = mockStatic(PlanUtils3.class)) {
    //      visitor.apply(scanRelation);
    //      planUtils3MockedStatic.verify(
    //          () -> PlanUtils3.fromDataSourceV2Relation(datasetFactory, openLineageContext,
    // relation),
    //          times(1));
    //    }
  }

  @Test
  void testApplyV2ScanRelation() {
    // FIXME: TEST LATER
    //    DataSourceV2Relation relation = mock(DataSourceV2Relation.class);
    //    DataSourceV2ScanRelation scanRelation = mock(DataSourceV2ScanRelation.class);
    //    when(scanRelation.relation()).thenReturn(relation);
    //
    //    try (MockedStatic planUtils3MockedStatic = mockStatic(PlanUtils3.class)) {
    //      visitor.apply(scanRelation);
    //      planUtils3MockedStatic.verify(
    //          () -> PlanUtils3.fromDataSourceV2Relation(datasetFactory, openLineageContext,
    // relation),
    //          times(1));
    //    }
  }

  @Test
  void testApplyDatasetVersionFacet() {
    // FIXME: TEST LATER
    //    DataSourceV2Relation relation = mock(DataSourceV2Relation.class);
    //    SparkListenerEvent event = mock(SparkListenerEvent.class);
    //
    //    Map<String, OpenLineage.DatasetFacet> expectedFacets = new HashMap<>();
    //    when(openLineageContext.getOpenLineage()).thenReturn(openLineage);
    //    when(openLineage.newDatasetVersionDatasetFacet(SOME_VERSION)).thenReturn(facet);
    //    expectedFacets.put("datasetVersion", facet);
    //
    //    try (MockedStatic planUtils3MockedStatic = mockStatic(PlanUtils3.class)) {
    //      try (MockedStatic facetUtilsMockedStatic =
    //          mockStatic(DatasetVersionDatasetFacetUtils.class)) {
    //        try (MockedStatic planUtils = mockStatic(PlanUtils.class)) {
    //
    // when(DatasetVersionDatasetFacetUtils.extractVersionFromDataSourceV2Relation(relation))
    //              .thenReturn(Optional.of(SOME_VERSION));
    //          when(PlanUtils.shouldIncludeDatasetVersionFacet(false, event)).thenReturn(true);
    //          visitor.setTriggeringEvent(event);
    //          visitor.apply(relation);
    //          planUtils3MockedStatic.verify(
    //              () ->
    //                  PlanUtils3.fromDataSourceV2Relation(
    //                      datasetFactory, openLineageContext, relation, expectedFacets),
    //              times(1));
    //        }
    //      }
    //    }
  }
}
