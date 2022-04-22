package io.openlineage.spark3.agent.lifecycle.plan.columnLineage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class ColumnLevelUtilsTest {

  @Test
  public void testRewriteOutputDataset() {
    OpenLineage.OutputDataset outputDataset = mock(OpenLineage.OutputDataset.class);
    OpenLineage.OutputDatasetOutputFacets datasetOutputFacets =
        mock(OpenLineage.OutputDatasetOutputFacets.class);
    OpenLineage.ColumnLineageDatasetFacet columnLineageDatasetFacet =
        mock(OpenLineage.ColumnLineageDatasetFacet.class);
    OpenLineage.DatasetFacets datasetFacets = mock(OpenLineage.DatasetFacets.class);

    OpenLineage.DocumentationDatasetFacet documentationDatasetFacet =
        mock(OpenLineage.DocumentationDatasetFacet.class);
    OpenLineage.SchemaDatasetFacet schemaDatasetFacet = mock(OpenLineage.SchemaDatasetFacet.class);
    OpenLineage.DatasetVersionDatasetFacet datasetVersionDatasetFacet =
        mock(OpenLineage.DatasetVersionDatasetFacet.class);
    OpenLineage.StorageDatasetFacet storageDatasetFacet =
        mock(OpenLineage.StorageDatasetFacet.class);
    OpenLineage.DatasourceDatasetFacet datasourceDatasetFacet =
        mock(OpenLineage.DatasourceDatasetFacet.class);
    OpenLineage.LifecycleStateChangeDatasetFacet lifecycleStateChangeDatasetFacet =
        mock(OpenLineage.LifecycleStateChangeDatasetFacet.class);
    OpenLineage.DatasetFacet datasetFacet = mock(OpenLineage.DatasetFacet.class);
    Map<String, OpenLineage.DatasetFacet> additionalProperties =
        Collections.singletonMap("key", datasetFacet);

    when(outputDataset.getName()).thenReturn("name");
    when(outputDataset.getNamespace()).thenReturn("namespace");
    when(outputDataset.getOutputFacets()).thenReturn(datasetOutputFacets);
    when(outputDataset.getFacets()).thenReturn(datasetFacets);

    when(datasetFacets.getDocumentation()).thenReturn(documentationDatasetFacet);
    when(datasetFacets.getDataSource()).thenReturn(datasourceDatasetFacet);
    when(datasetFacets.getSchema()).thenReturn(schemaDatasetFacet);
    when(datasetFacets.getVersion()).thenReturn(datasetVersionDatasetFacet);
    when(datasetFacets.getStorage()).thenReturn(storageDatasetFacet);
    when(datasetFacets.getLifecycleStateChange()).thenReturn(lifecycleStateChangeDatasetFacet);
    when(datasetFacets.getAdditionalProperties()).thenReturn(additionalProperties);

    OpenLineage.OutputDataset rewriteOutputDataset =
        ColumnLevelLineageUtils.rewriteOutputDataset(outputDataset, columnLineageDatasetFacet);

    assertEquals("name", rewriteOutputDataset.getName());
    assertEquals("namespace", rewriteOutputDataset.getNamespace());
    assertEquals(datasetOutputFacets, rewriteOutputDataset.getOutputFacets());

    assertEquals(documentationDatasetFacet, rewriteOutputDataset.getFacets().getDocumentation());
    assertEquals(schemaDatasetFacet, rewriteOutputDataset.getFacets().getSchema());
    assertEquals(datasetVersionDatasetFacet, rewriteOutputDataset.getFacets().getVersion());
    assertEquals(storageDatasetFacet, rewriteOutputDataset.getFacets().getStorage());
    assertEquals(datasourceDatasetFacet, rewriteOutputDataset.getFacets().getDataSource());
    assertEquals(
        lifecycleStateChangeDatasetFacet,
        rewriteOutputDataset.getFacets().getLifecycleStateChange());
    assertEquals(columnLineageDatasetFacet, rewriteOutputDataset.getFacets().getColumnLineage());
    assertEquals(
        datasetFacet, rewriteOutputDataset.getFacets().getAdditionalProperties().get("key"));
  }
}
