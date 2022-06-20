package io.openlineage.flink.api;

import io.openlineage.client.OpenLineage;

public abstract class DatasetFactory<D extends OpenLineage.Dataset> {
  private final OpenLineage openLineage;

  private DatasetFactory(OpenLineage openLineage) {
    this.openLineage = openLineage;
  }

  abstract OpenLineage.Builder<D> datasetBuilder(
      String name, String namespace, OpenLineage.DatasetFacets datasetFacet);

  public static DatasetFactory<OpenLineage.InputDataset> input(OpenLineage client) {
    return new DatasetFactory<>(client) {
      public OpenLineage.Builder<OpenLineage.InputDataset> datasetBuilder(
          String name, String namespace, OpenLineage.DatasetFacets datasetFacet) {
        return client.newInputDatasetBuilder().namespace(namespace).name(name).facets(datasetFacet);
      }
    };
  }

  public static DatasetFactory<OpenLineage.OutputDataset> output(OpenLineage client) {
    return new DatasetFactory<>(client) {
      public OpenLineage.Builder<OpenLineage.OutputDataset> datasetBuilder(
          String name, String namespace, OpenLineage.DatasetFacets datasetFacet) {
        return client
            .newOutputDatasetBuilder()
            .namespace(namespace)
            .name(name)
            .facets(datasetFacet);
      }
    };
  }

  public OpenLineage.DatasetFacetsBuilder getDatasetFacetsBuilder() {
    return openLineage.newDatasetFacetsBuilder();
  }

  public D getDataset(
      String name, String namespace, OpenLineage.DatasetFacetsBuilder facetsBuilder) {
    return getDataset(name, namespace, facetsBuilder.build());
  }

  public D getDataset(String name, String namespace) {
    return getDataset(name, namespace, getDatasetFacetsBuilder().build());
  }

  public D getDataset(String name, String namespace, OpenLineage.DatasetFacets datasetFacet) {
    return datasetBuilder(name, namespace, datasetFacet).build();
  }
}
