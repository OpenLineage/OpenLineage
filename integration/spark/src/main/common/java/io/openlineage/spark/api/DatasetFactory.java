/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.api;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.plan.LogicalRelationDatasetBuilder;
import io.openlineage.spark.agent.lifecycle.plan.columnLineage.ColumnLevelLineageUtils;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.agent.util.PlanUtils;
import java.net.URI;
import org.apache.spark.sql.types.StructType;

/**
 * Defines factories for creating either {@link io.openlineage.client.OpenLineage.InputDataset}s or
 * {@link io.openlineage.client.OpenLineage.OutputDataset}s. This allows {@link QueryPlanVisitor}s
 * that may identify input or output datasets (e.g., a {@link
 * io.openlineage.spark.agent.lifecycle.plan.BigQueryNodeVisitor} or {@link
 * LogicalRelationDatasetBuilder}) to be reused in the construction of both input and output
 * datasets, allowing each to focus on extracting the identifier and general {@link
 * io.openlineage.client.OpenLineage.DatasetFacet}s, while delegating to the factory to construct
 * the correct instance.
 *
 * <p>Ideally, this would be a sealed class. We emulate that by using a private constructor and
 * provide two static factory methods - {@link #input(OpenLineageContext)} and {@link
 * #output(OpenLineageContext)}.
 *
 * @param <D> the implementation of {@link io.openlineage.client.OpenLineage.Dataset} constructed by
 *     this factory
 */
public abstract class DatasetFactory<D extends OpenLineage.Dataset> {
  private final OpenLineageContext context;

  private DatasetFactory(OpenLineageContext context) {
    this.context = context;
  }

  abstract OpenLineage.Builder<D> datasetBuilder(
      String name, String namespace, OpenLineage.DatasetFacets datasetFacet);

  /**
   * Create a {@link DatasetFactory} that constructs only {@link
   * io.openlineage.client.OpenLineage.InputDataset}s.
   *
   * @param context
   * @return
   */
  public static DatasetFactory<OpenLineage.InputDataset> input(OpenLineageContext context) {
    return new DatasetFactory<OpenLineage.InputDataset>(context) {
      public OpenLineage.Builder<OpenLineage.InputDataset> datasetBuilder(
          String name, String namespace, OpenLineage.DatasetFacets datasetFacet) {
        return context
            .getOpenLineage()
            .newInputDatasetBuilder()
            .namespace(namespace)
            .name(name)
            .facets(datasetFacet);
      }
    };
  }

  /**
   * Create a {@link DatasetFactory} that constructs only {@link
   * io.openlineage.client.OpenLineage.OutputDataset}s.
   *
   * @param context
   * @return
   */
  public static DatasetFactory<OpenLineage.OutputDataset> output(OpenLineageContext context) {
    return new DatasetFactory<OpenLineage.OutputDataset>(context) {
      public OpenLineage.Builder<OpenLineage.OutputDataset> datasetBuilder(
          String name, String namespace, OpenLineage.DatasetFacets datasetFacet) {
        return context
            .getOpenLineage()
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
    OpenLineage.DatasetFacets datasetFacet = datasetFacet(schema, namespace);
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
  public D getDataset(
      URI outputPath, StructType schema, OpenLineage.DatasetFacetsBuilder datasetFacetsBuilder) {
    String namespace = PlanUtils.namespaceUri(outputPath);
    datasetFacetsBuilder
        .schema(PlanUtils.schemaFacet(context.getOpenLineage(), schema))
        .dataSource(PlanUtils.datasourceFacet(context.getOpenLineage(), namespace));

    return getDataset(outputPath.getPath(), namespace, datasetFacetsBuilder.build());
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
    OpenLineage.DatasetFacets datasetFacet = datasetFacet(schema, namespace);
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
    OpenLineage.DatasetFacets datasetFacet = datasetFacet(schema, ident.getNamespace());

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
      // OpenLineageContext context,
      DatasetIdentifier ident,
      StructType schema,
      OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange lifecycleStateChange) {
    OpenLineage.DatasetFacetsBuilder builder =
        context
            .getOpenLineage()
            .newDatasetFacetsBuilder()
            .schema(PlanUtils.schemaFacet(context.getOpenLineage(), schema))
            .lifecycleStateChange(
                context
                    .getOpenLineage()
                    .newLifecycleStateChangeDatasetFacet(lifecycleStateChange, null))
            .dataSource(PlanUtils.datasourceFacet(context.getOpenLineage(), ident.getNamespace()));

    ColumnLevelLineageUtils.buildColumnLineageDatasetFacet(context, schema)
        .ifPresent(facet -> builder.columnLineage(facet));

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
   * @param schema
   * @param namespaceUri
   * @return
   */
  private OpenLineage.DatasetFacets datasetFacet(StructType schema, String namespaceUri) {
    OpenLineage.DatasetFacetsBuilder datasetFacetsBuilder =
        context
            .getOpenLineage()
            .newDatasetFacetsBuilder()
            .schema(PlanUtils.schemaFacet(context.getOpenLineage(), schema))
            .dataSource(PlanUtils.datasourceFacet(context.getOpenLineage(), namespaceUri));

    ColumnLevelLineageUtils.buildColumnLineageDatasetFacet(context, schema)
        .ifPresent(facet -> datasetFacetsBuilder.columnLineage(facet));

    return datasetFacetsBuilder.build();
  }
}
