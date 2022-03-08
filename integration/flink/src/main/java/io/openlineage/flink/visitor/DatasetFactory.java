package io.openlineage.flink.visitor;

import io.openlineage.client.OpenLineage;
import java.net.URI;

public abstract class DatasetFactory<D extends OpenLineage.Dataset> {
  private final OpenLineage openLineage;

  private DatasetFactory(OpenLineage openLineage) {
    this.openLineage = openLineage;
  }

  abstract OpenLineage.Builder<D> datasetBuilder(
      String name, String namespace, OpenLineage.DatasetFacets datasetFacet);

  public static DatasetFactory<OpenLineage.InputDataset> input(OpenLineage client) {
    return new DatasetFactory<OpenLineage.InputDataset>(client) {
      public OpenLineage.Builder<OpenLineage.InputDataset> datasetBuilder(
          String name, String namespace, OpenLineage.DatasetFacets datasetFacet) {
        return client.newInputDatasetBuilder().namespace(namespace).name(name).facets(datasetFacet);
      }
    };
  }

  public static DatasetFactory<OpenLineage.OutputDataset> output(OpenLineage client) {
    return new DatasetFactory<OpenLineage.OutputDataset>(client) {
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

  public D getDataset(String name, String namespace) {
    OpenLineage.DatasetFacetsBuilder builder =
        openLineage
            .newDatasetFacetsBuilder()
            .dataSource(
                openLineage
                    .newDatasourceDatasetFacetBuilder()
                    .uri(URI.create(""))
                    .name(namespace)
                    .build());
    return getDataset(name, namespace, builder.build());
  }

  public D getDataset(String name, String namespace, OpenLineage.DatasetFacets datasetFacet) {
    return datasetBuilder(name, namespace, datasetFacet).build();
  }
}
