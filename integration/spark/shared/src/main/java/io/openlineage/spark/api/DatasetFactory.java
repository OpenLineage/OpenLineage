/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange;
import io.openlineage.spark.agent.lifecycle.plan.BigQueryNodeVisitor;
import io.openlineage.spark.agent.lifecycle.plan.LogicalRelationDatasetBuilder;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.agent.util.PlanUtils;
import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.types.StructType;

/**
 * Defines factories for creating either {@link OpenLineage.InputDataset}s or {@link
 * OpenLineage.OutputDataset}s. This allows {@link QueryPlanVisitor}s that may identify input or
 * output datasets (e.g., a {@link BigQueryNodeVisitor} or {@link LogicalRelationDatasetBuilder}) to
 * be reused in the construction of both input and output datasets, allowing each to focus on
 * extracting the identifier and general {@link OpenLineage.DatasetFacet}s, while delegating to the
 * factory to construct the correct instance.
 *
 * <p>Ideally, this would be a sealed class. We emulate that by using a private constructor and
 * provide two static factory methods - {@link #input(OpenLineageContext)} and {@link
 * #output(OpenLineageContext)}.
 *
 * @param <D> the implementation of {@link OpenLineage.Dataset} constructed by this factory
 */
public abstract class DatasetFactory<D extends OpenLineage.Dataset> {
  private final OpenLineageContext context;

  private DatasetFactory(OpenLineageContext context) {
    this.context = context;
  }

  abstract OpenLineage.Builder<D> datasetBuilder(
      String name, String namespace, OpenLineage.DatasetFacets datasetFacet);

  /**
   * Create a {@link DatasetFactory} that constructs only {@link OpenLineage.InputDataset}s.
   *
   * @param context
   * @return
   */
  public static DatasetFactory<OpenLineage.InputDataset> input(OpenLineageContext context) {
    return new DatasetFactory<OpenLineage.InputDataset>(context) {
      @Override
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
   * Create a {@link DatasetFactory} that constructs only {@link OpenLineage.OutputDataset}s.
   *
   * @param context
   * @return
   */
  public static DatasetFactory<OpenLineage.OutputDataset> output(OpenLineageContext context) {
    return new DatasetFactory<OpenLineage.OutputDataset>(context) {
      @Override
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
    return datasetBuilder(name, namespace, datasetFacetBuilder(schema, namespace).build()).build();
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

    return getDataset(new DatasetIdentifier(outputPath.getPath(), namespace), datasetFacetsBuilder);
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
    return getDataset(
        new DatasetIdentifier(outputPath.getPath(), namespace),
        datasetFacetBuilder(schema, namespace));
  }

  /**
   * Construct a {@link OpenLineage.Dataset} with the given {@link DatasetIdentifier} and schema.
   *
   * @param ident
   * @param schema
   * @return
   */
  public D getDataset(DatasetIdentifier ident, StructType schema) {
    OpenLineage.DatasetFacetsBuilder facetsBuilder =
        datasetFacetBuilder(schema, ident.getNamespace());
    includeSymlinksFacet(facetsBuilder, ident);
    return getDataset(ident, facetsBuilder);
  }

  /**
   * Construct a {@link OpenLineage.Dataset} with the given {@link DatasetIdentifier}, schema and
   * facets map.
   *
   * @param ident
   * @param schema
   * @param lifecycleStateChange
   * @return
   */
  public D getDataset(
      DatasetIdentifier ident, StructType schema, LifecycleStateChange lifecycleStateChange) {
    OpenLineage.DatasetFacetsBuilder facetsBuilder =
        datasetFacetBuilder(schema, ident.getNamespace());
    facetsBuilder.lifecycleStateChange(
        context.getOpenLineage().newLifecycleStateChangeDatasetFacet(lifecycleStateChange, null));
    includeSymlinksFacet(facetsBuilder, ident);
    return getDataset(new DatasetIdentifier(ident.getName(), ident.getNamespace()), facetsBuilder);
  }

  private void includeSymlinksFacet(
      OpenLineage.DatasetFacetsBuilder builder, DatasetIdentifier di) {
    if (!di.getSymlinks().isEmpty()) {
      List<OpenLineage.SymlinksDatasetFacetIdentifiers> symlinks =
          di.getSymlinks().stream()
              .map(
                  symlink ->
                      context
                          .getOpenLineage()
                          .newSymlinksDatasetFacetIdentifiersBuilder()
                          .name(symlink.getName())
                          .namespace(symlink.getNamespace())
                          .type(symlink.getType().toString())
                          .build())
              .collect(Collectors.toList());

      builder.symlinks(context.getOpenLineage().newSymlinksDatasetFacet(symlinks));
    }
  }

  /**
   * Construct a {@link OpenLineage.Dataset} with the given {@link DatasetIdentifier} and {@link
   * OpenLineage.DatasetFacets}.
   *
   * @param ident
   * @param facetsBuilder
   * @return
   */
  public D getDataset(DatasetIdentifier ident, OpenLineage.DatasetFacetsBuilder facetsBuilder) {
    includeSymlinksFacet(facetsBuilder, ident);
    return datasetBuilder(ident.getName(), ident.getNamespace(), facetsBuilder.build()).build();
  }

  /**
   * Construct a {@link OpenLineage.DatasetFacets} given a schema and a namespace.
   *
   * @param schema
   * @param namespaceUri
   * @return
   */
  private OpenLineage.DatasetFacetsBuilder datasetFacetBuilder(
      StructType schema, String namespaceUri) {
    return context
        .getOpenLineage()
        .newDatasetFacetsBuilder()
        .schema(PlanUtils.schemaFacet(context.getOpenLineage(), schema))
        .dataSource(PlanUtils.datasourceFacet(context.getOpenLineage(), namespaceUri));
  }
}
