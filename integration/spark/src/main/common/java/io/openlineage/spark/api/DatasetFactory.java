package io.openlineage.spark.api;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.agent.util.PlanUtils;
import java.net.URI;
import org.apache.spark.sql.types.StructType;

/**
 *
 * @param <D>
 */
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

  /**
   * Construct a dataset {@link OpenLineage.Dataset} given a name, namespace, and {@link StructType}
   * schema.
   *
   * @param name
   * @param namespace
   * @param schema
   * @return
   */
  public D getDataset(String name, String namespace, StructType schema) {
    OpenLineage.DatasetFacets datasetFacet = datasetFacet(openLineage, schema, namespace);
    return datasetBuilder(name, namespace, datasetFacet).build();
  }

  /**
   * Construct a dataset {@link OpenLineage.Dataset} given a name, namespace, and preconstructed
   * {@link OpenLineage.DatasetFacets}.
   *
   * @param name
   * @param namespace
   * @param datasetFacet
   * @return
   */
  public D getDataset(String name, String namespace, OpenLineage.DatasetFacets datasetFacet) {
    return datasetBuilder(name, namespace, datasetFacet).build();
  }

  /**
   * Given a {@link URI}, construct a valid {@link OpenLineage.Dataset} following the expected
   * naming conventions.
   *
   * @param outputPath
   * @param schema
   * @return
   */
  public D getDataset(URI outputPath, StructType schema) {
    String namespace = PlanUtils.namespaceUri(outputPath);
    OpenLineage.DatasetFacets datasetFacet = datasetFacet(openLineage, schema, namespace);
    return getDataset(outputPath.getPath(), namespace, datasetFacet);
  }

  public D getDataset(DatasetIdentifier ident, StructType schema) {
    OpenLineage.DatasetFacets datasetFacet = datasetFacet(openLineage, schema, ident.getNamespace());
    return getDataset(ident.getName(), ident.getNamespace(), datasetFacet);
  }

  public D getDataset(DatasetIdentifier ident, OpenLineage.DatasetFacets datasetFacet) {
    return getDataset(ident.getName(), ident.getNamespace(), datasetFacet);
  }

  /**
   * Construct a {@link OpenLineage.DatasetFacets} given a schema and a namespace.
   *
   * @param openLineage
   * @param schema
   * @param namespaceUri
   * @return
   */
  private static OpenLineage.DatasetFacets datasetFacet(OpenLineage openLineage, StructType schema,
      String namespaceUri) {
    return openLineage
        .newDatasetFacetsBuilder()
        .schema(PlanUtils.schemaFacet(openLineage, schema))
        .dataSource(PlanUtils.datasourceFacet(openLineage, namespaceUri))
        .build();
  }
}
