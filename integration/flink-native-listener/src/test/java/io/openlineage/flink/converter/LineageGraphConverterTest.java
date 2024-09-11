/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.converter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.DatasetFacet;
import io.openlineage.client.OpenLineage.DatasetFacetsBuilder;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.JobTypeJobFacet;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.OwnershipJobFacetOwners;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.DatasetIdentifier.Symlink;
import io.openlineage.client.utils.DatasetIdentifier.SymlinkType;
import io.openlineage.flink.client.OpenLineageContext;
import io.openlineage.flink.client.OpenLineageContext.JobIdentifier;
import io.openlineage.flink.client.Versions;
import io.openlineage.flink.config.FlinkOpenLineageConfig;
import io.openlineage.flink.config.FlinkOpenLineageConfig.JobConfig;
import io.openlineage.flink.config.FlinkOpenLineageConfig.JobOwnersConfig;
import io.openlineage.flink.visitor.VisitorFactory;
import io.openlineage.flink.visitor.facet.DatasetFacetVisitor;
import io.openlineage.flink.visitor.identifier.DatasetIdentifierVisitor;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageGraph;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Test class for {@link LineageGraphConverter} */
class LineageGraphConverterTest {

  private static UUID runUuid = UUID.randomUUID();
  VisitorFactory visitorFactory = mock(VisitorFactory.class);
  OpenLineageContext context;
  LineageGraphConverter converter;
  FlinkOpenLineageConfig config = mock(FlinkOpenLineageConfig.class);
  JobConfig jobConfig = mock(JobConfig.class);
  LineageGraph graph = mock(LineageGraph.class);

  @BeforeEach
  public void setup() {
    context =
        OpenLineageContext.builder()
            .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
            .client(mock(OpenLineageClient.class))
            .config(config)
            .runId(runUuid)
            .build();

    context.setJobId(JobIdentifier.builder().build());
    converter = new LineageGraphConverter(context, visitorFactory);

    when(config.getJob()).thenReturn(jobConfig);
  }

  @Test
  void testJobOwnership() {
    JobOwnersConfig ownersConfig = new JobOwnersConfig();
    ownersConfig.getAdditionalProperties().put("team", "MyTeam");
    ownersConfig.getAdditionalProperties().put("person", "John Smith");

    when(jobConfig.getOwners()).thenReturn(ownersConfig);

    List<OwnershipJobFacetOwners> owners =
        converter.convert(graph, EventType.START).getJob().getFacets().getOwnership().getOwners();
    assertThat(owners).hasSize(2);
    assertThat(owners.stream().filter(o -> "team".equals(o.getType())).findAny().get().getName())
        .isEqualTo("MyTeam");
    assertThat(owners.stream().filter(o -> "person".equals(o.getType())).findAny().get().getName())
        .isEqualTo("John Smith");
  }

  @Test
  void testJobType() {
    SourceLineageVertex source1 = mock(SourceLineageVertex.class);
    SourceLineageVertex source2 = mock(SourceLineageVertex.class);

    when(source1.boundedness()).thenReturn(Boundedness.CONTINUOUS_UNBOUNDED);
    when(source2.boundedness()).thenReturn(Boundedness.BOUNDED);

    when(graph.sources()).thenReturn(Arrays.asList(source1, source2));

    JobTypeJobFacet jobTypeFacet =
        converter.convert(graph, EventType.START).getJob().getFacets().getJobType();
    assertThat(jobTypeFacet.getIntegration()).isEqualTo("FLINK");
    assertThat(jobTypeFacet.getJobType()).isEqualTo("JOB");
    assertThat(jobTypeFacet.getProcessingType()).isEqualTo("STREAMING");

    when(source1.boundedness()).thenReturn(Boundedness.BOUNDED);
    assertThat(
            converter
                .convert(graph, EventType.START)
                .getJob()
                .getFacets()
                .getJobType()
                .getProcessingType())
        .isEqualTo("BATCH");
  }

  @Test
  void testFacetVisitors() {
    SourceLineageVertex source = mock(SourceLineageVertex.class);
    LineageDataset sourceDataset = mock(LineageDataset.class);
    DatasetFacet openlineageSourceFacet = mock(DatasetFacet.class);

    when(source.datasets()).thenReturn(Arrays.asList(sourceDataset));
    when(sourceDataset.name()).thenReturn("sourceName");
    when(sourceDataset.namespace()).thenReturn("sourceNamespace");

    LineageVertex sink = mock(LineageVertex.class);
    LineageDataset sinkDataset = mock(LineageDataset.class);
    DatasetFacet openlineageSinkFacet = mock(DatasetFacet.class);

    when(sink.datasets()).thenReturn(Arrays.asList(sinkDataset));
    when(sinkDataset.name()).thenReturn("sinkName");
    when(sinkDataset.namespace()).thenReturn("sinkNamespace");

    when(graph.sources()).thenReturn(Arrays.asList(source));
    when(graph.sinks()).thenReturn(Arrays.asList(sink));

    when(visitorFactory.loadDatasetFacetVisitors(context))
        .thenReturn(
            Arrays.asList(
                new TestingDatasetFacetVisitor(
                    Map.of(
                        sourceDataset,
                        openlineageSourceFacet,
                        sinkDataset,
                        openlineageSinkFacet))));
    when(visitorFactory.loadDatasetIdentifierVisitors(context)).thenReturn(Collections.emptyList());
    LineageGraphConverter converter = new LineageGraphConverter(context, visitorFactory);

    List<InputDataset> inputs = converter.convert(graph, EventType.START).getInputs();
    assertThat(inputs).hasSize(1);
    assertThat(inputs.get(0))
        .hasFieldOrPropertyWithValue("name", "sourceName")
        .hasFieldOrPropertyWithValue("namespace", "sourceNamespace");
    assertThat(inputs.get(0).getFacets().getAdditionalProperties().get("facet"))
        .isEqualTo(openlineageSourceFacet);

    List<OutputDataset> outputs = converter.convert(graph, EventType.START).getOutputs();
    assertThat(outputs).hasSize(1);
    assertThat(outputs.get(0))
        .hasFieldOrPropertyWithValue("name", "sinkName")
        .hasFieldOrPropertyWithValue("namespace", "sinkNamespace");
    assertThat(outputs.get(0).getFacets().getAdditionalProperties().get("facet"))
        .isEqualTo(openlineageSinkFacet);
  }

  @Test
  void testDatasetIdentifierFacetVisitors() {
    SourceLineageVertex source = mock(SourceLineageVertex.class);
    LineageDataset dataset = mock(LineageDataset.class);
    when(source.datasets()).thenReturn(Arrays.asList(dataset));

    when(graph.sources()).thenReturn(Arrays.asList(source));
    when(graph.sinks()).thenReturn(Collections.emptyList());

    when(visitorFactory.loadDatasetIdentifierVisitors(context))
        .thenReturn(Arrays.asList(new TestingDatasetIdentifierVisitor()));

    LineageGraphConverter converter = new LineageGraphConverter(context, visitorFactory);
    List<InputDataset> inputs = converter.convert(graph, EventType.START).getInputs();
    assertThat(inputs).hasSize(2);
    assertThat(
            inputs.stream()
                .map(id -> new DatasetIdentifier(id.getName(), id.getNamespace()))
                .collect(Collectors.toList()))
        .containsExactlyInAnyOrder(
            new DatasetIdentifier("datasetName1", "namespace"),
            new DatasetIdentifier("datasetName2", "namespace"));

    assertThat(inputs.get(0).getFacets().getSymlinks().getIdentifiers().get(0))
        .hasFieldOrPropertyWithValue("namespace", "namespace")
        .hasFieldOrPropertyWithValue("name", "table1")
        .hasFieldOrPropertyWithValue("type", "TABLE");
  }

  private static class TestingDatasetFacetVisitor implements DatasetFacetVisitor {

    private final Map<LineageDataset, DatasetFacet> behaviour;

    private TestingDatasetFacetVisitor(Map<LineageDataset, DatasetFacet> behaviour) {
      this.behaviour = behaviour;
    }

    @Override
    public boolean isDefinedAt(LineageDataset dataset) {
      return behaviour.containsKey(dataset);
    }

    @Override
    public void apply(LineageDataset dataset, DatasetFacetsBuilder builder) {
      builder.put("facet", behaviour.get(dataset));
    }
  }

  private static class TestingDatasetIdentifierVisitor implements DatasetIdentifierVisitor {
    @Override
    public boolean isDefinedAt(LineageDataset dataset) {
      return true;
    }

    @Override
    public Collection<DatasetIdentifier> apply(LineageDataset dataset) {
      return Arrays.asList(
          new DatasetIdentifier(
              "datasetName1",
              "namespace",
              Arrays.asList(new Symlink("table1", "namespace", SymlinkType.TABLE))),
          new DatasetIdentifier("datasetName2", "namespace"));
    }
  }
}
