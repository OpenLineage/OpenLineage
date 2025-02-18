/*
/* Copyright 2018-2025 contributors to the OpenLineage project
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
import io.openlineage.client.job.JobConfig;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.DatasetIdentifier.Symlink;
import io.openlineage.client.utils.DatasetIdentifier.SymlinkType;
import io.openlineage.flink.client.OpenLineageContext;
import io.openlineage.flink.client.OpenLineageContext.JobIdentifier;
import io.openlineage.flink.client.Versions;
import io.openlineage.flink.config.FlinkOpenLineageConfig;
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
import org.apache.flink.streaming.api.lineage.LineageDatasetFacet;
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

    when(config.getJobConfig()).thenReturn(jobConfig);
  }

  @Test
  void testJobOwnership() {
    JobConfig.JobOwnersConfig ownersConfig = new JobConfig.JobOwnersConfig();
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
    SourceLineageVertex source1 =
        sourceVertexOf(Boundedness.CONTINUOUS_UNBOUNDED, Collections.emptyList());
    SourceLineageVertex source2 = sourceVertexOf(Boundedness.BOUNDED, Collections.emptyList());

    when(graph.sources()).thenReturn(Arrays.asList(source1, source2));

    JobTypeJobFacet jobTypeFacet =
        converter.convert(graph, EventType.START).getJob().getFacets().getJobType();
    assertThat(jobTypeFacet.getIntegration()).isEqualTo("FLINK");
    assertThat(jobTypeFacet.getJobType()).isEqualTo("JOB");
    assertThat(jobTypeFacet.getProcessingType()).isEqualTo("STREAMING");

    when(graph.sources()).thenReturn(Arrays.asList(source2, source2));
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
    DatasetFacet openlineageSourceFacet = mock(DatasetFacet.class);
    LineageDatasetWithIdentifier sourceDataset = lineageDatasetOf("sourceName", "sourceNamespace");
    SourceLineageVertex source =
        sourceVertexOf(
            Boundedness.CONTINUOUS_UNBOUNDED,
            Collections.singletonList(sourceDataset.getFlinkDataset()));

    DatasetFacet openlineageSinkFacet = mock(DatasetFacet.class);
    LineageDatasetWithIdentifier sinkDataset = lineageDatasetOf("sinkName", "sinkNamespace");
    LineageVertex sink =
        new LineageVertex() {
          @Override
          public List<LineageDataset> datasets() {
            return List.of(sinkDataset.getFlinkDataset());
          }
        };

    when(graph.sources()).thenReturn(List.of(source));
    when(graph.sinks()).thenReturn(List.of(sink));

    when(visitorFactory.loadDatasetFacetVisitors(context))
        .thenReturn(
            List.of(
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
    LineageDatasetWithIdentifier dataset = lineageDatasetOf("datasetName", "namespace");
    SourceLineageVertex source =
        sourceVertexOf(
            Boundedness.CONTINUOUS_UNBOUNDED, Collections.singletonList(dataset.getFlinkDataset()));

    when(graph.sources()).thenReturn(List.of(source));
    when(graph.sinks()).thenReturn(Collections.emptyList());

    when(visitorFactory.loadDatasetIdentifierVisitors(context))
        .thenReturn(List.of(new TestingDatasetIdentifierVisitor()));

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

  private LineageDatasetWithIdentifier lineageDatasetOf(String name, String namespace) {
    return new LineageDatasetWithIdentifier(
        new DatasetIdentifier(name, namespace),
        new LineageDataset() {
          @Override
          public String name() {
            return name;
          }

          @Override
          public String namespace() {
            return namespace;
          }

          @Override
          public Map<String, LineageDatasetFacet> facets() {
            return Collections.emptyMap();
          }
        });
  }

  private SourceLineageVertex sourceVertexOf(
      Boundedness boundedness, List<LineageDataset> datasets) {
    return new SourceLineageVertex() {
      @Override
      public Boundedness boundedness() {
        return boundedness;
      }

      @Override
      public List<LineageDataset> datasets() {
        return datasets;
      }
    };
  }

  private static class TestingDatasetFacetVisitor implements DatasetFacetVisitor {

    private final Map<LineageDatasetWithIdentifier, DatasetFacet> behaviour;

    private TestingDatasetFacetVisitor(Map<LineageDatasetWithIdentifier, DatasetFacet> behaviour) {
      this.behaviour = behaviour;
    }

    @Override
    public boolean isDefinedAt(LineageDatasetWithIdentifier dataset) {
      return behaviour.containsKey(dataset);
    }

    @Override
    public void apply(LineageDatasetWithIdentifier dataset, DatasetFacetsBuilder builder) {
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
