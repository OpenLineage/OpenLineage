/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.DatasetVersionDatasetFacetUtils;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.FileIndex;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.internal.SessionState;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class LogicalRelationDatasetBuilderTest {

  private static final String SOME_VERSION = "version_1";
  OpenLineageContext openLineageContext = mock(OpenLineageContext.class);
  SparkSession session = mock(SparkSession.class);
  LogicalRelationDatasetBuilder visitor =
      new LogicalRelationDatasetBuilder(
          openLineageContext, DatasetFactory.output(openLineageContext), false);
  OpenLineage.DatasetVersionDatasetFacet facet = mock(OpenLineage.DatasetVersionDatasetFacet.class);
  OpenLineage openLineage = mock(OpenLineage.class);

  @Test
  void testApplyForHadoopFsRelationDatasetVersionFacet() {
    HadoopFsRelation hadoopFsRelation = mock(HadoopFsRelation.class);
    LogicalRelation logicalRelation = mock(LogicalRelation.class);
    Configuration hadoopConfig = mock(Configuration.class);
    SparkContext sparkContext = mock(SparkContext.class);
    SessionState sessionState = mock(SessionState.class);
    FileIndex fileIndex = mock(FileIndex.class);
    Path path = new Path("/tmp/path1");

    when(logicalRelation.relation()).thenReturn(hadoopFsRelation);
    when(openLineageContext.getSparkContext()).thenReturn(sparkContext);
    when(openLineageContext.getSparkSession()).thenReturn(Optional.of(session));
    when(openLineageContext.getOpenLineage())
        .thenReturn(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI));
    when(facet.getDatasetVersion()).thenReturn(SOME_VERSION);
    when(session.sessionState()).thenReturn(sessionState);
    when(sessionState.newHadoopConfWithOptions(any())).thenReturn(hadoopConfig);
    when(hadoopFsRelation.location()).thenReturn(fileIndex);
    when(fileIndex.rootPaths())
        .thenReturn(
            scala.collection.JavaConverters.collectionAsScalaIterableConverter(Arrays.asList(path))
                .asScala()
                .toSeq());
    when(openLineage.newDatasetFacetsBuilder()).thenReturn(new OpenLineage.DatasetFacetsBuilder());
    when(openLineage.newDatasetVersionDatasetFacet(SOME_VERSION)).thenReturn(facet);

    try (MockedStatic mocked = mockStatic(PlanUtils.class)) {
      try (MockedStatic mockedFacetUtils = mockStatic(DatasetVersionDatasetFacetUtils.class)) {
        when(PlanUtils.getDirectoryPath(path, hadoopConfig)).thenReturn(new Path("/tmp"));
        when(DatasetVersionDatasetFacetUtils.extractVersionFromLogicalRelation(logicalRelation))
            .thenReturn(Optional.of(SOME_VERSION));

        List<OpenLineage.Dataset> datasets = visitor.apply(logicalRelation);
        assertEquals(1, datasets.size());
        OpenLineage.Dataset ds = datasets.get(0);
        assertEquals("/tmp", ds.getName());
        assertEquals(SOME_VERSION, ds.getFacets().getVersion().getDatasetVersion());
      }
    }
  }
}
