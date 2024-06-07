/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.api;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.dataset.namespace.resolver.DatasetNamespaceCombinedResolver;
import io.openlineage.client.utils.DatasetIdentifier;

public abstract class DatasetFactory<D extends OpenLineage.Dataset> {
  private final OpenLineage openLineage;
  protected final DatasetNamespaceCombinedResolver namespaceResolver;

  private DatasetFactory(
      OpenLineage openLineage, DatasetNamespaceCombinedResolver namespaceResolver) {
    this.openLineage = openLineage;
    this.namespaceResolver = namespaceResolver;
  }

  abstract OpenLineage.Builder<D> datasetBuilder(
      String name, String namespace, OpenLineage.DatasetFacets datasetFacet);

  public static DatasetFactory<OpenLineage.InputDataset> input(OpenLineageContext context) {
    return new DatasetFactory<>(
        context.getOpenLineage(), new DatasetNamespaceCombinedResolver(context.getConfig())) {
      @Override
      public OpenLineage.Builder<OpenLineage.InputDataset> datasetBuilder(
          String name, String namespace, OpenLineage.DatasetFacets datasetFacet) {
        return context
            .getOpenLineage()
            .newInputDatasetBuilder()
            .namespace(namespaceResolver.resolve(namespace))
            .name(name)
            .facets(datasetFacet);
      }
    };
  }

  public static DatasetFactory<OpenLineage.OutputDataset> output(OpenLineageContext context) {
    return new DatasetFactory<>(
        context.getOpenLineage(), new DatasetNamespaceCombinedResolver(context.getConfig())) {
      @Override
      public OpenLineage.Builder<OpenLineage.OutputDataset> datasetBuilder(
          String name, String namespace, OpenLineage.DatasetFacets datasetFacet) {
        return context
            .getOpenLineage()
            .newOutputDatasetBuilder()
            .namespace(namespaceResolver.resolve(namespace))
            .name(name)
            .facets(datasetFacet);
      }
    };
  }

  public OpenLineage.DatasetFacetsBuilder getDatasetFacetsBuilder() {
    return openLineage.newDatasetFacetsBuilder();
  }

  public D getDataset(DatasetIdentifier di, OpenLineage.DatasetFacetsBuilder facetsBuilder) {
    return getDataset(di.getName(), di.getNamespace(), facetsBuilder.build());
  }

  public D getDataset(
      String name, String namespace, OpenLineage.DatasetFacetsBuilder facetsBuilder) {
    return getDataset(name, namespace, facetsBuilder.build());
  }

  public D getDataset(String name, String namespace) {
    return getDataset(name, namespace, getDatasetFacetsBuilder().build());
  }

  public D getDataset(String name, String namespace, OpenLineage.DatasetFacets datasetFacet) {
    return datasetBuilder(name, namespaceResolver.resolve(namespace), datasetFacet).build();
  }
}
