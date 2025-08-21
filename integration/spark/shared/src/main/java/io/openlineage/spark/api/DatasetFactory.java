/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

import static io.openlineage.client.OpenLineage.*;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.Builder;
import io.openlineage.client.OpenLineage.DatasetFacets;
import io.openlineage.client.OpenLineage.DatasetFacetsBuilder;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.dataset.DatasetCompositeFacetsBuilder;
import io.openlineage.client.dataset.namespace.resolver.DatasetNamespaceCombinedResolver;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.lifecycle.plan.BigQueryNodeOutputVisitor;
import io.openlineage.spark.agent.lifecycle.plan.LogicalRelationDatasetBuilder;
import io.openlineage.spark.agent.util.DatasetVersionUtils;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.PlanUtils;
import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.types.StructType;

/**
 * Defines factories for creating either {@link OpenLineage.InputDataset}s or {@link
 * OpenLineage.OutputDataset}s. This allows {@link QueryPlanVisitor}s that may identify input or
 * output datasets (e.g., a {@link BigQueryNodeOutputVisitor} or {@link
 * LogicalRelationDatasetBuilder}) to be reused in the construction of both input and output
 * datasets, allowing each to focus on extracting the identifier and general {@link DatasetFacet}s,
 * while delegating to the factory to construct the correct instance.
 *
 * <p>Ideally, this would be a sealed class. We emulate that by using a private constructor and
 * provide two static factory methods - {@link #input(OpenLineageContext)} and {@link
 * #output(OpenLineageContext)}.
 *
 * <p>{@link DatasetCompositeFacetsBuilder} should deprecate usage of {@link DatasetFacetsBuilder}
 * in OpenLineage 1.28. Currently, both are supported.
 *
 * @param <D> the implementation of {@link Dataset} constructed by this factory
 */
public abstract class DatasetFactory<D extends Dataset> {
  private final OpenLineageContext context;
  protected final DatasetNamespaceCombinedResolver namespaceResolver;

  private DatasetFactory(OpenLineageContext context) {
    this.context = context;
    this.namespaceResolver = new DatasetNamespaceCombinedResolver(context.getOpenLineageConfig());
  }

  abstract OpenLineage.Builder<D> datasetBuilder(
      String name, String namespace, DatasetCompositeFacetsBuilder facetsBuilder);

  @Deprecated
  abstract OpenLineage.Builder<D> datasetBuilder(
      String name, String namespace, DatasetFacets facets);

  public abstract void buildVersionFacets(
      DatasetCompositeFacetsBuilder facetsBuilder, String version);

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
          String name, String namespace, DatasetCompositeFacetsBuilder facetsBuilder) {
        return context
            .getOpenLineage()
            .newInputDatasetBuilder()
            .namespace(namespaceResolver.resolve(namespace))
            .name(name)
            .inputFacets(facetsBuilder.getInputFacets().build())
            .facets(facetsBuilder.getFacets().build());
      }

      @Deprecated
      @Override
      Builder<InputDataset> datasetBuilder(String name, String namespace, DatasetFacets facets) {
        return context
            .getOpenLineage()
            .newInputDatasetBuilder()
            .namespace(namespaceResolver.resolve(namespace))
            .name(name)
            .facets(facets);
      }

      @Override
      public void buildVersionFacets(DatasetCompositeFacetsBuilder facetsBuilder, String version) {
        DatasetVersionUtils.buildVersionFacets(context, facetsBuilder, version);
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
          String name, String namespace, DatasetCompositeFacetsBuilder facetsBuilder) {
        return context
            .getOpenLineage()
            .newOutputDatasetBuilder()
            .namespace(namespaceResolver.resolve(namespace))
            .name(name)
            .outputFacets(facetsBuilder.getOutputFacets().build())
            .facets(facetsBuilder.getFacets().build());
      }

      @Deprecated
      @Override
      Builder<OutputDataset> datasetBuilder(String name, String namespace, DatasetFacets facets) {
        return context
            .getOpenLineage()
            .newOutputDatasetBuilder()
            .namespace(namespaceResolver.resolve(namespace))
            .name(name)
            .facets(facets);
      }

      @Override
      public void buildVersionFacets(DatasetCompositeFacetsBuilder facetsBuilder, String version) {
        DatasetVersionUtils.buildVersionOutputFacets(context, facetsBuilder, version);
      }
    };
  }

  /**
   * Construct a dataset {@link Dataset} given a name, namespace, and {@link StructType} schema.
   *
   * @param name
   * @param namespace
   * @param schema
   * @return
   */
  public D getDataset(String name, String namespace, StructType schema) {
    DatasetCompositeFacetsBuilder facetsBuilder = createCompositeFacetBuilder();
    facetsBuilder
        .getFacets()
        .dataSource(
            PlanUtils.datasourceFacet(
                context.getOpenLineage(), namespaceResolver.resolve(namespace)))
        .schema(PlanUtils.schemaFacet(context.getOpenLineage(), schema));

    return datasetBuilder(name, namespace, facetsBuilder).build();
  }

  /**
   * Given a {@link URI}, construct a valid {@link Dataset} following the expected naming
   * conventions.
   *
   * @param outputPath
   * @param schema
   * @return
   */
  @Deprecated
  public D getDataset(
      URI outputPath, StructType schema, DatasetFacetsBuilder datasetFacetsBuilder) {
    DatasetCompositeFacetsBuilder compositeFacetsBuilder =
        new DatasetCompositeFacetsBuilder(context.openLineage);
    compositeFacetsBuilder.setFacets(datasetFacetsBuilder);
    return getDataset(outputPath, schema, compositeFacetsBuilder);
  }

  /**
   * Given a {@link URI}, construct a valid {@link Dataset} following the expected naming
   * conventions.
   *
   * @param outputPath
   * @param schema
   * @return
   */
  public D getDataset(
      URI outputPath, StructType schema, DatasetCompositeFacetsBuilder datasetFacetsBuilder) {
    DatasetIdentifier datasetIdentifier = PathUtils.fromURI(outputPath);
    datasetFacetsBuilder
        .getFacets()
        .schema(PlanUtils.schemaFacet(context.getOpenLineage(), schema))
        .dataSource(
            PlanUtils.datasourceFacet(
                context.getOpenLineage(),
                namespaceResolver.resolve(datasetIdentifier.getNamespace())));

    return getDataset(datasetIdentifier, datasetFacetsBuilder);
  }

  /**
   * Given a {@link URI}, construct a valid {@link Dataset} following the expected naming
   * conventions.
   *
   * @param outputPath
   * @param schema
   * @return
   */
  public D getDataset(URI outputPath, StructType schema) {
    String namespace = PlanUtils.namespaceUri(outputPath);
    DatasetCompositeFacetsBuilder facetsBuilder = createCompositeFacetBuilder();
    facetsBuilder
        .getFacets()
        .schema(PlanUtils.schemaFacet(context.getOpenLineage(), schema))
        .dataSource(
            PlanUtils.datasourceFacet(
                context.getOpenLineage(), namespaceResolver.resolve(namespace)));

    return getDataset(PathUtils.fromURI(outputPath), facetsBuilder);
  }

  /**
   * Construct a {@link Dataset} with the given {@link DatasetIdentifier} and schema.
   *
   * @param ident
   * @param schema
   * @return
   */
  public D getDataset(DatasetIdentifier ident, StructType schema) {
    DatasetCompositeFacetsBuilder facetsBuilder = datasetFacetBuilder(schema, ident.getNamespace());
    includeSymlinksFacet(facetsBuilder, ident);
    return getDataset(ident, facetsBuilder);
  }

  /**
   * Construct a {@link Dataset} with the given {@link DatasetIdentifier}, schema and catalog facet.
   *
   * @param ident
   * @param schema
   * @param catalogDatasetFacet
   * @return
   */
  public D getDataset(
      DatasetIdentifier ident, StructType schema, CatalogDatasetFacet catalogDatasetFacet) {
    DatasetCompositeFacetsBuilder facetsBuilder = datasetFacetBuilder(schema, ident.getNamespace());
    includeSymlinksFacet(facetsBuilder, ident);
    includeCatalogFacet(facetsBuilder, catalogDatasetFacet);
    return getDataset(ident, facetsBuilder);
  }

  /**
   * Construct a {@link Dataset} with the given {@link DatasetIdentifier}, schema and facets map.
   *
   * @param ident
   * @param schema
   * @param lifecycleStateChange
   * @return
   */
  public D getDataset(
      DatasetIdentifier ident, StructType schema, LifecycleStateChange lifecycleStateChange) {
    DatasetCompositeFacetsBuilder facetsBuilder = datasetFacetBuilder(schema, ident.getNamespace());
    facetsBuilder
        .getFacets()
        .lifecycleStateChange(
            context
                .getOpenLineage()
                .newLifecycleStateChangeDatasetFacet(lifecycleStateChange, null));
    includeSymlinksFacet(facetsBuilder, ident);
    return getDataset(new DatasetIdentifier(ident.getName(), ident.getNamespace()), facetsBuilder);
  }

  /**
   * Construct a {@link Dataset} with the given {@link DatasetIdentifier}, schema and facets map.
   *
   * @param ident
   * @param schema
   * @param lifecycleStateChange
   * @param catalogDatasetFacet
   * @return
   */
  public D getDataset(
      DatasetIdentifier ident,
      StructType schema,
      LifecycleStateChange lifecycleStateChange,
      CatalogDatasetFacet catalogDatasetFacet) {
    DatasetCompositeFacetsBuilder facetsBuilder = datasetFacetBuilder(schema, ident.getNamespace());
    facetsBuilder
        .getFacets()
        .lifecycleStateChange(
            context
                .getOpenLineage()
                .newLifecycleStateChangeDatasetFacet(lifecycleStateChange, null));
    includeSymlinksFacet(facetsBuilder, ident);
    includeCatalogFacet(facetsBuilder, catalogDatasetFacet);
    return getDataset(new DatasetIdentifier(ident.getName(), ident.getNamespace()), facetsBuilder);
  }

  private void includeCatalogFacet(
      DatasetCompositeFacetsBuilder builder, CatalogDatasetFacet catalogDatasetFacet) {
    builder.getFacets().catalog(catalogDatasetFacet);
  }

  private void includeSymlinksFacet(DatasetCompositeFacetsBuilder builder, DatasetIdentifier di) {
    if (!di.getSymlinks().isEmpty()) {
      List<SymlinksDatasetFacetIdentifiers> symlinks =
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

      builder.getFacets().symlinks(context.getOpenLineage().newSymlinksDatasetFacet(symlinks));
    }
  }

  /**
   * Construct a {@link Dataset} with the given {@link DatasetIdentifier} and {@link
   * OpenLineage.DatasetFacets}.
   *
   * @param ident
   * @param facetsBuilder
   * @return
   */
  public D getDataset(DatasetIdentifier ident, DatasetCompositeFacetsBuilder facetsBuilder) {
    includeSymlinksFacet(facetsBuilder, ident);
    return datasetBuilder(ident.getName(), ident.getNamespace(), facetsBuilder).build();
  }

  public D getDataset(String name, String namespace) {
    return datasetBuilder(name, namespace, datasetFacetBuilder(namespace)).build();
  }

  /**
   * Construct a {@link OpenLineage.DatasetFacets} given a schema and a namespace.
   *
   * @param schema
   * @param namespaceUri
   * @return
   */
  private DatasetCompositeFacetsBuilder datasetFacetBuilder(
      StructType schema, String namespaceUri) {
    DatasetCompositeFacetsBuilder builder = createCompositeFacetBuilder();
    builder
        .getFacets()
        .schema(PlanUtils.schemaFacet(context.getOpenLineage(), schema))
        .dataSource(
            PlanUtils.datasourceFacet(
                context.getOpenLineage(), namespaceResolver.resolve(namespaceUri)));
    return builder;
  }

  private DatasetCompositeFacetsBuilder datasetFacetBuilder(String namespaceUri) {
    DatasetCompositeFacetsBuilder builder = createCompositeFacetBuilder();
    builder
        .getFacets()
        .dataSource(
            PlanUtils.datasourceFacet(
                context.getOpenLineage(), namespaceResolver.resolve(namespaceUri)));

    return builder;
  }

  public DatasetCompositeFacetsBuilder createCompositeFacetBuilder() {
    return new DatasetCompositeFacetsBuilder(context.getOpenLineage());
  }
}
