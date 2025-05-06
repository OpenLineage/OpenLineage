/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.converter;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.DatasetFacetsBuilder;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.dataset.namespace.resolver.DatasetNamespaceCombinedResolver;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.visitor.Flink2VisitorFactory;
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
  private final DatasetNamespaceCombinedResolver namespaceResolver;

  OpenLineageDatasetExtractor(OpenLineageContext context, Flink2VisitorFactory visitorFactory) {
    this.context = context;
    this.facetVisitors = visitorFactory.loadDatasetFacetVisitors(context);
    this.identifierVisitors = visitorFactory.loadDatasetIdentifierVisitors(context);
    this.namespaceResolver = new DatasetNamespaceCombinedResolver(context.getConfig());
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
                    .namespace(d.getDatasetIdentifier().getNamespace())
                    .name(d.getDatasetIdentifier().getName())
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
                    .namespace(d.getDatasetIdentifier().getNamespace())
                    .name(d.getDatasetIdentifier().getName())
                    .facets(convert(d))
                    .build())
        .collect(Collectors.toList());
  }

  private OpenLineage.DatasetFacets convert(LineageDatasetWithIdentifier dataset) {
    DatasetFacetsBuilder facetsBuilder = new DatasetFacetsBuilder();

    if (dataset.getDatasetIdentifier().getSymlinks() != null) {
      facetsBuilder.symlinks(
          context
              .getOpenLineage()
              .newSymlinksDatasetFacet(
                  dataset.getDatasetIdentifier().getSymlinks().stream()
                      .map(
                          i ->
                              context
                                  .getOpenLineage()
                                  .newSymlinksDatasetFacetIdentifiers(
                                      i.getNamespace(), i.getName(), i.getType().toString()))
                      .collect(Collectors.toList())));
    }

    facetVisitors.stream()
        .filter(v -> v.isDefinedAt(dataset))
        .forEach(v -> v.apply(dataset, facetsBuilder));

    return facetsBuilder.build();
  }

  private Collection<LineageDatasetWithIdentifier> extractDatasetsWithIdentifiers(
      LineageDataset dataset) {
    List<DatasetIdentifierVisitor> visitors =
        identifierVisitors.stream()
            .filter(v -> v.isDefinedAt(dataset))
            .collect(Collectors.toList());

    if (visitors.isEmpty()) {
      // no visitors to be applied
      return Collections.singletonList(
          new LineageDatasetWithIdentifier(
              namespaceResolver.resolve(new DatasetIdentifier(dataset.name(), dataset.namespace())),
              dataset));
    }

    return identifierVisitors.stream()
        .filter(v -> v.isDefinedAt(dataset))
        .flatMap(v -> v.apply(dataset).stream())
        .map(namespaceResolver::resolve)
        .map(di -> new LineageDatasetWithIdentifier(di, dataset))
        .collect(Collectors.toList());
  }
}
