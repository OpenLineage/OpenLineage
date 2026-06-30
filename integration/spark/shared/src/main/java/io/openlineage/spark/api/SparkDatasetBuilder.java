/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.CatalogDatasetFacet;
import io.openlineage.client.OpenLineage.DatasetVersionDatasetFacet;
import io.openlineage.client.OpenLineage.DatasourceDatasetFacet;
import io.openlineage.client.OpenLineage.LifecycleStateChangeDatasetFacet;
import io.openlineage.client.OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange;
import io.openlineage.client.OpenLineage.SchemaDatasetFacet;
import io.openlineage.client.OpenLineage.SymlinksDatasetFacet;
import io.openlineage.client.dataset.DatasetCompositeFacetsBuilder;
import io.openlineage.client.dataset.namespace.resolver.DatasetNamespaceCombinedResolver;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.util.CatalogDatasetFacetUtils;
import io.openlineage.spark.agent.util.DatasetVersionUtils;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.PlanUtils;
import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.types.StructType;

/**
 * A context-aware wrapper around {@link DatasetCompositeFacetsBuilder} that provides convenience
 * overloads for facet-setter methods requiring an {@link OpenLineageContext}.
 *
 * <p>For example, {@link #schema(StructType)} converts a Spark {@link StructType} to a {@link
 * SchemaDatasetFacet} using {@link PlanUtils#schemaFacet} internally, so callers do not need to
 * hold a reference to the {@link OpenLineage} instance themselves.
 *
 * <p>All methods return {@code this} for fluent chaining. The underlying {@link
 * DatasetCompositeFacetsBuilder} is accessible via {@link #getInner()} for passing to existing APIs
 * that still require it.
 */
public abstract class SparkDatasetBuilder<T extends OpenLineage.Dataset> {

  protected final OpenLineageContext context;
  protected final DatasetNamespaceCombinedResolver namespaceResolver;

  @Getter protected final DatasetCompositeFacetsBuilder inner;
  @Getter protected String name;
  @Getter protected String namespace;

  public SparkDatasetBuilder(OpenLineageContext context) {
    this(context, new DatasetCompositeFacetsBuilder(context.getOpenLineage()));
  }

  public SparkDatasetBuilder(OpenLineageContext context, DatasetCompositeFacetsBuilder inner) {
    this.context = context;
    this.namespaceResolver = new DatasetNamespaceCombinedResolver(context.getOpenLineageConfig());
    this.inner = inner;
  }

  public SparkDatasetBuilder<T> dataset(DatasetIdentifier datasetIdentifier) {
    this.name = datasetIdentifier.getName();
    this.namespace = datasetIdentifier.getNamespace();
    dataSource(datasetIdentifier.getNamespace());
    symlink(datasetIdentifier.getSymlinks());
    return this;
  }

  public SparkDatasetBuilder<T> dataset(String name, String namespace) {
    return dataset(new DatasetIdentifier(name, namespace));
  }

  public SparkDatasetBuilder<T> dataset(String name, URI outputPath) {
    return dataset(new DatasetIdentifier(name, PlanUtils.namespaceUri(outputPath)));
  }

  public SparkDatasetBuilder<T> dataset(URI outputPath) {
    return dataset(PathUtils.fromURI(outputPath));
  }

  public SparkDatasetBuilder<T> dataset(CatalogTable catalogTable) {
    if (context.getSparkSession().isPresent()) {
      dataset(PathUtils.fromCatalogTable(catalogTable, context.getSparkSession().get()));
      catalog(catalogTable.identifier());
    }
    return this;
  }

  public SparkDatasetBuilder<T> symlink(SymlinksDatasetFacet symlinksFacet) {
    inner.getFacets().symlinks(symlinksFacet);
    return this;
  }

  public SparkDatasetBuilder<T> symlink(List<DatasetIdentifier.Symlink> symlinks) {
    if (!symlinks.isEmpty()) {
      List<OpenLineage.SymlinksDatasetFacetIdentifiers> symlinkIdentifiers =
          symlinks.stream()
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
      symlinks(
          context
              .getOpenLineage()
              .newSymlinksDatasetFacetBuilder()
              .identifiers(symlinkIdentifiers)
              .build());
    }
    return this;
  }

  public SparkDatasetBuilder<T> schema(SchemaDatasetFacet schemaFacet) {
    inner.getFacets().schema(schemaFacet);
    return this;
  }

  public SparkDatasetBuilder<T> schema(StructType schema) {
    return schema(PlanUtils.schemaFacet(context.getOpenLineage(), schema));
  }

  public SparkDatasetBuilder<T> dataSource(DatasourceDatasetFacet datasourceFacet) {
    inner.getFacets().dataSource(datasourceFacet);
    return this;
  }

  public SparkDatasetBuilder<T> dataSource(String namespace) {
    return dataSource(
        PlanUtils.datasourceFacet(context.getOpenLineage(), namespaceResolver.resolve(namespace)));
  }

  public SparkDatasetBuilder<T> catalog(CatalogDatasetFacet catalogFacet) {
    inner.getFacets().catalog(catalogFacet);
    return this;
  }

  public SparkDatasetBuilder<T> catalog(TableIdentifier identifier) {
    if (context.getSparkSession().isPresent()) {
      if (CatalogDatasetFacetUtils.isHiveCatalog(context.getSparkSession().get(), identifier)) {
        catalog();
      }
    }
    return this;
  }

  public SparkDatasetBuilder<T> catalog() {
    CatalogDatasetFacetUtils.getCatalogDatasetFacetForHive(context).ifPresent(this::catalog);
    return this;
  }

  public SparkDatasetBuilder<T> symlinks(SymlinksDatasetFacet symlinksFacet) {
    inner.getFacets().symlinks(symlinksFacet);
    return this;
  }

  public SparkDatasetBuilder<T> lifecycleStateChange(
      LifecycleStateChangeDatasetFacet lifecycleFacet) {
    inner.getFacets().lifecycleStateChange(lifecycleFacet);
    return this;
  }

  public SparkDatasetBuilder<T> lifecycleStateChange(LifecycleStateChange lifecycleStateChange) {
    return lifecycleStateChange(lifecycleStateChange, null);
  }

  public SparkDatasetBuilder<T> lifecycleStateChange(
      LifecycleStateChange lifecycleStateChange, String previousName, String previousNamespace) {
    return lifecycleStateChange(
        lifecycleStateChange,
        context
            .getOpenLineage()
            .newLifecycleStateChangeDatasetFacetPreviousIdentifierBuilder()
            .name(previousName)
            .namespace(previousNamespace)
            .build());
  }

  public SparkDatasetBuilder<T> lifecycleStateChange(
      LifecycleStateChange lifecycleStateChange,
      OpenLineage.LifecycleStateChangeDatasetFacetPreviousIdentifier previousIdentifier) {
    return lifecycleStateChange(
        context
            .getOpenLineage()
            .newLifecycleStateChangeDatasetFacetBuilder()
            .lifecycleStateChange(lifecycleStateChange)
            .previousIdentifier(previousIdentifier)
            .build());
  }

  /**
   * Sets the dataset version facet. For output datasets, also triggers vendor-specific output facet
   * builders (e.g. Iceberg snapshot facets). Subclasses may override to provide richer behaviour.
   */
  public SparkDatasetBuilder<T> version(String version) {
    DatasetVersionUtils.buildVersionFacets(context, inner, version);
    return this;
  }

  /** Sets a pre-built {@link DatasetVersionDatasetFacet} directly on the facets builder. */
  public SparkDatasetBuilder<T> version(DatasetVersionDatasetFacet versionFacet) {
    inner.getFacets().version(versionFacet);
    return this;
  }

  public abstract T build();

  public abstract SparkDatasetBuilder<T> fromBuilder(
      OpenLineage.DatasetFacetsBuilder facetsBuilder);
}
