/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.api;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.agent.util.PlanUtils;
import java.net.URI;
import org.apache.spark.sql.types.StructType;

/**
 * Defines factories for creating either {@link io.openlineage.client.OpenLineage.InputDataset}s or
 * {@link io.openlineage.client.OpenLineage.OutputDataset}s. This allows {@link QueryPlanVisitor}s
 * that may identify input or output datasets (e.g., a {@link
 * io.openlineage.spark.agent.lifecycle.plan.BigQueryNodeVisitor} or {@link
 * io.openlineage.spark.agent.lifecycle.plan.LogicalRelationVisitor}) to be reused in the
 * construction of both input and output datasets, allowing each to focus on extracting the
 * identifier and general {@link io.openlineage.client.OpenLineage.DatasetFacet}s, while delegating
 * to the factory to construct the correct instance.
 *
 * <p>Ideally, this would be a sealed class. We emulate that by using a private constructor and
 * provide two static factory methods - {@link #input(OpenLineage)} and {@link
 * #output(OpenLineage)}.
 *
 * @param <D> the implementation of {@link io.openlineage.client.OpenLineage.Dataset} constructed by
 *     this factory
 */
public abstract class DatasetFactory<D extends OpenLineage.Dataset> {
  private final OpenLineage openLineage;

  private DatasetFactory(OpenLineage openLineage) {
    this.openLineage = openLineage;
  }

  abstract OpenLineage.Builder<D> datasetBuilder(
      String name, String namespace, OpenLineage.DatasetFacets datasetFacet);

  /**
   * Create a {@link DatasetFactory} that constructs only {@link
   * io.openlineage.client.OpenLineage.InputDataset}s.
   *
   * @param client
   * @return
   */
  public static DatasetFactory<OpenLineage.InputDataset> input(OpenLineage client) {
    return new DatasetFactory<OpenLineage.InputDataset>(client) {
      public OpenLineage.Builder<OpenLineage.InputDataset> datasetBuilder(
          String name, String namespace, OpenLineage.DatasetFacets datasetFacet) {
        return client.newInputDatasetBuilder().namespace(namespace).name(name).facets(datasetFacet);
      }
    };
  }

  /**
   * Create a {@link DatasetFactory} that constructs only {@link
   * io.openlineage.client.OpenLineage.OutputDataset}s.
   *
   * @param client
   * @return
   */
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

  /**
   * Construct a {@link io.openlineage.client.OpenLineage.Dataset} with the given {@link
   * DatasetIdentifier} and schema.
   *
   * @param ident
   * @param schema
   * @return
   */
  public D getDataset(DatasetIdentifier ident, StructType schema) {
    OpenLineage.DatasetFacets datasetFacet =
        datasetFacet(openLineage, schema, ident.getNamespace());
    return getDataset(ident.getName(), ident.getNamespace(), datasetFacet);
  }

  /**
   * Construct a {@link io.openlineage.client.OpenLineage.Dataset} with the given {@link
   * DatasetIdentifier}, schema and facets map.
   *
   * @param ident
   * @param schema
   * @param lifecycleStateChange
   * @return
   */
  public D getDataset(
      DatasetIdentifier ident,
      StructType schema,
      OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange lifecycleStateChange) {
    OpenLineage.DatasetFacetsBuilder builder =
        openLineage
            .newDatasetFacetsBuilder()
            .schema(PlanUtils.schemaFacet(openLineage, schema))
            .lifecycleStateChange(
                openLineage.newLifecycleStateChangeDatasetFacet(lifecycleStateChange, null))
            .dataSource(PlanUtils.datasourceFacet(openLineage, ident.getNamespace()));
    return getDataset(ident.getName(), ident.getNamespace(), builder.build());
  }

  /**
   * Construct a {@link io.openlineage.client.OpenLineage.Dataset} with the given {@link
   * DatasetIdentifier} and {@link OpenLineage.DatasetFacets}.
   *
   * @param ident
   * @param datasetFacet
   * @return
   */
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
  private static OpenLineage.DatasetFacets datasetFacet(
      OpenLineage openLineage, StructType schema, String namespaceUri) {
    return openLineage
        .newDatasetFacetsBuilder()
        .schema(PlanUtils.schemaFacet(openLineage, schema))
        .dataSource(PlanUtils.datasourceFacet(openLineage, namespaceUri))
        .build();
  }
}
