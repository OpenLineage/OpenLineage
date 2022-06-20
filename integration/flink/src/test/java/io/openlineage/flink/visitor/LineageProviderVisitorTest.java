package io.openlineage.flink.visitor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.agent.client.EventEmitter;
import io.openlineage.flink.api.DatasetFactory;
import io.openlineage.flink.api.OpenLineageContext;
import java.util.Collections;
import java.util.List;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class LineageProviderVisitorTest {
  OpenLineageContext context = mock(OpenLineageContext.class);
  LineageProviderVisitor<OpenLineage.InputDataset> visitor;
  OpenLineage openLineage = new OpenLineage(EventEmitter.OPEN_LINEAGE_CLIENT_URI);

  @BeforeEach
  @SneakyThrows
  public void setup() {
    when(context.getOpenLineage()).thenReturn(openLineage);
    visitor = new LineageProviderVisitor<>(context, DatasetFactory.input(openLineage));
  }

  @Test
  public void testIsDefined() {
    assertEquals(false, visitor.isDefinedAt(mock(Object.class)));
    assertEquals(true, visitor.isDefinedAt(mock(ExampleLineageProvider.class)));
  }

  @Test
  public void testApply() {
    OpenLineage.SchemaDatasetFacet schemaDatasetFacet =
        openLineage.newSchemaDatasetFacet(
            Collections.singletonList(
                openLineage.newSchemaDatasetFacetFields("a", "INTEGER", "desc")));
    ExampleLineageProvider provider =
        new ExampleLineageProvider("name", "namespace", schemaDatasetFacet);

    List<OpenLineage.InputDataset> facets = visitor.apply(provider);

    assertThat(facets).hasSize(1);

    OpenLineage.Dataset facet = facets.get(0);

    assertThat(facet.getName()).isEqualTo("name");
    assertThat(facet.getNamespace()).isEqualTo("namespace");
    assertThat(facet.getFacets().getSchema().getFields().size()).isEqualTo(1);
    assertThat(facet.getFacets().getSchema().getFields().get(0))
        .isEqualTo(schemaDatasetFacet.getFields().get(0));
  }
}
