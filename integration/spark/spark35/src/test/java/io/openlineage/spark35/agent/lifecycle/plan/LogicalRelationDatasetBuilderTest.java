/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark35.agent.lifecycle.plan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.lifecycle.SparkOpenLineageExtensionVisitorWrapper;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.HoodieBaseRelation;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.storage.StoragePath;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Option;

class LogicalRelationDatasetBuilderTest {

  private final OpenLineageContext openLineageContext = mock(OpenLineageContext.class);
  private final SparkOpenLineageExtensionVisitorWrapper visitorWrapper =
      mock(SparkOpenLineageExtensionVisitorWrapper.class);
  private LogicalRelationDatasetBuilder<OpenLineage.OutputDataset> builder;

  @BeforeEach
  void setUp() {
    when(openLineageContext.getOpenLineage())
        .thenReturn(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI));
    when(openLineageContext.getSparkExtensionVisitorWrapper()).thenReturn(visitorWrapper);
    when(visitorWrapper.isDefinedAt(org.mockito.ArgumentMatchers.any())).thenReturn(false);
    when(openLineageContext.getQueryExecution())
        .thenReturn(Optional.of(mock(QueryExecution.class)));
    builder =
        new LogicalRelationDatasetBuilder<>(
            openLineageContext, DatasetFactory.output(openLineageContext), false);
  }

  @Test
  void testApplyForHudiBaseRelationCoversMorRelations() {
    HoodieBaseRelation hudiRelation = mock(HoodieBaseRelation.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    StoragePath storagePath = mock(StoragePath.class);
    LogicalRelation logicalRelation = mock(LogicalRelation.class);

    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("name", StringType$.MODULE$, false, Metadata.empty())
            });

    when(logicalRelation.relation()).thenReturn(hudiRelation);
    when(logicalRelation.catalogTable()).thenReturn(Option.empty());
    when(hudiRelation.basePath()).thenReturn(new Path("/tmp/hudi_mor"));
    when(hudiRelation.schema()).thenReturn(schema);
    when(hudiRelation.metaClient()).thenReturn(metaClient);
    when(metaClient.getTableType()).thenReturn(HoodieTableType.MERGE_ON_READ);
    when(metaClient.getBasePathV2()).thenReturn(storagePath);
    when(storagePath.toUri()).thenReturn(URI.create("file:/tmp/hudi_mor"));

    List<OpenLineage.OutputDataset> datasets =
        builder.apply(mock(SparkListenerEvent.class), logicalRelation);

    assertThat(datasets).hasSize(1);
    OpenLineage.OutputDataset dataset = datasets.get(0);
    assertThat(dataset.getNamespace()).isEqualTo("file");
    assertThat(dataset.getName()).isEqualTo("/tmp/hudi_mor");
    assertThat(dataset.getFacets().getSymlinks()).isNotNull();
  }
}
