/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

import static io.openlineage.client.OpenLineage.*;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.dataset.DatasetCompositeFacetsBuilder;
import io.openlineage.client.dataset.namespace.resolver.DatasetNamespaceCombinedResolver;
import io.openlineage.spark.agent.lifecycle.plan.BigQueryNodeOutputVisitor;
import io.openlineage.spark.agent.lifecycle.plan.LogicalRelationDatasetBuilder;
import io.openlineage.spark.agent.util.DatasetVersionUtils;

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

  public abstract SparkDatasetBuilder<D> sparkDatasetBuilder();

  public abstract SparkDatasetBuilder<D> sparkDatasetBuilder(DatasetCompositeFacetsBuilder inner);

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
      public void buildVersionFacets(DatasetCompositeFacetsBuilder facetsBuilder, String version) {
        DatasetVersionUtils.buildVersionFacets(context, facetsBuilder, version);
      }

      @Override
      public SparkDatasetBuilder<InputDataset> sparkDatasetBuilder() {
        return new SparkInputDatasetBuilder(context);
      }

      @Override
      public SparkDatasetBuilder<InputDataset> sparkDatasetBuilder(
          DatasetCompositeFacetsBuilder inner) {
        return new SparkInputDatasetBuilder(context, inner);
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
      public void buildVersionFacets(DatasetCompositeFacetsBuilder facetsBuilder, String version) {
        DatasetVersionUtils.buildVersionOutputFacets(context, facetsBuilder, version);
      }

      @Override
      public SparkDatasetBuilder<OutputDataset> sparkDatasetBuilder() {
        return new SparkOutputDatasetBuilder(context);
      }

      @Override
      public SparkDatasetBuilder<OutputDataset> sparkDatasetBuilder(
          DatasetCompositeFacetsBuilder inner) {
        return new SparkOutputDatasetBuilder(context, inner);
      }
    };
  }

  public DatasetCompositeFacetsBuilder createCompositeFacetBuilder() {
    return new DatasetCompositeFacetsBuilder(context.getOpenLineage());
  }
}
