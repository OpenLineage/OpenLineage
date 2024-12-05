/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.converter;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.DatasetFacetsBuilder;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.flink.client.OpenLineageContext;
import io.openlineage.flink.visitor.VisitorFactory;
import io.openlineage.flink.visitor.facet.DatasetFacetVisitor;
import io.openlineage.flink.visitor.identifier.DatasetIdentifierVisitor;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageGraph;

/** Class used to extract datasets from Flink lineage graph. */
class OpenLineageDatasetExtractor {
  private final OpenLineageContext context;
  private final Collection<DatasetFacetVisitor> facetVisitors;
  private final Collection<DatasetIdentifierVisitor> identifierVisitors;

  OpenLineageDatasetExtractor(OpenLineageContext context, VisitorFactory visitorFactory) {
    this.context = context;
    this.facetVisitors = visitorFactory.loadDatasetFacetVisitors(context);
    this.identifierVisitors = visitorFactory.loadDatasetIdentifierVisitors(context);
  }

  List<InputDataset> extractInputs(LineageGraph graph) {
    if (graph == null) {
      return Collections.emptyList();
    }
    return graph.sources().stream()
        .flatMap(source -> source.datasets().stream())
        .flatMap(d -> extractDatasetsWithIdentifiers(d).stream())
        .map(
            d ->
                context
                    .getOpenLineage()
                    .newInputDatasetBuilder()
                    .namespace(d.datasetIdentifier.getNamespace())
                    .name(d.datasetIdentifier.getName())
                    .facets(convert(d))
                    .build())
        .collect(Collectors.toList());
  }

  List<OutputDataset> extractOutputs(LineageGraph graph) {
    if (graph == null) {
      return Collections.emptyList();
    }

    return graph.sinks().stream()
        .flatMap(sink -> sink.datasets().stream())
        .flatMap(d -> extractDatasetsWithIdentifiers(d).stream())
        .map(
            d ->
                context
                    .getOpenLineage()
                    .newOutputDatasetBuilder()
                    .namespace(d.datasetIdentifier.getNamespace())
                    .name(d.datasetIdentifier.getName())
                    .facets(convert(d))
                    .build())
        .collect(Collectors.toList());
  }

  private OpenLineage.DatasetFacets convert(DatasetWithDatasetIdentifier dataset) {
    DatasetFacetsBuilder facetsBuilder = new DatasetFacetsBuilder();

    if (dataset.datasetIdentifier.getSymlinks() != null) {
      facetsBuilder.symlinks(
          context
              .getOpenLineage()
              .newSymlinksDatasetFacet(
                  dataset.datasetIdentifier.getSymlinks().stream()
                      .map(
                          i ->
                              context
                                  .getOpenLineage()
                                  .newSymlinksDatasetFacetIdentifiers(
                                      i.getNamespace(), i.getName(), i.getType().toString()))
                      .collect(Collectors.toList())));
    }

    facetVisitors.stream()
        .filter(v -> v.isDefinedAt(dataset.dataset))
        .forEach(v -> v.apply(dataset.dataset, facetsBuilder));

    return facetsBuilder.build();
  }

  private static class DatasetWithDatasetIdentifier {
    DatasetIdentifier datasetIdentifier;
    LineageDataset dataset;

    DatasetWithDatasetIdentifier(DatasetIdentifier datasetIdentifier, LineageDataset dataset) {
      this.datasetIdentifier = datasetIdentifier;
      this.dataset = dataset;
    }
  }

  private Collection<DatasetWithDatasetIdentifier> extractDatasetsWithIdentifiers(
      LineageDataset dataset) {
    List<DatasetIdentifierVisitor> visitors =
        identifierVisitors.stream()
            .filter(v -> v.isDefinedAt(dataset))
            .collect(Collectors.toList());

    if (visitors.isEmpty()) {
      // no visitors to be applied
      return Collections.singletonList(
          new DatasetWithDatasetIdentifier(
              new DatasetIdentifier(dataset.name(), dataset.namespace()), dataset));
    }

    return identifierVisitors.stream()
        .filter(v -> v.isDefinedAt(dataset))
        .flatMap(v -> v.apply(dataset).stream())
        .map(di -> new DatasetWithDatasetIdentifier(di, dataset))
        .collect(Collectors.toList());
  }
}
