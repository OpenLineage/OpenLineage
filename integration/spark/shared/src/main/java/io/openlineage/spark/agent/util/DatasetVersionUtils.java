/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.client.OpenLineage.DatasetVersionDatasetFacet;
import io.openlineage.client.OpenLineage.OutputDatasetFacet;
import io.openlineage.client.dataset.DatasetCompositeFacetsBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import io.openlineage.spark.api.Vendors;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.function.BiConsumer;

/** Utility class for dataset versioning. */
public class DatasetVersionUtils {

  public static void buildVersionFacets(
      OpenLineageContext context, DatasetCompositeFacetsBuilder facetsBuilder, String version) {
    facetsBuilder
        .getFacets()
        .version(context.getOpenLineage().newDatasetVersionDatasetFacet(version))
        .build();
  }

  /**
   * Adds version facet to the dataset, it also applies all the custom facet builders which are
   * defined on a version string. This is particularly useful for vendors like Iceberg to add custom
   * facets to dataset.
   *
   * @param context
   * @param facetsBuilder
   * @param version
   */
  public static void buildVersionOutputFacets(
      OpenLineageContext context, DatasetCompositeFacetsBuilder facetsBuilder, String version) {

    DatasetVersionDatasetFacet datasetVersionDatasetFacet =
        context.getOpenLineage().newDatasetVersionDatasetFacet(version);

    facetsBuilder.getFacets().version(datasetVersionDatasetFacet).build();

    // vendors' output dataset facets builders on scan
    Collection<OpenLineageEventHandlerFactory> handlerFactories =
        Optional.ofNullable(context.getVendors())
            .map(Vendors::getEventHandlerFactories)
            .orElse(Collections.emptyList());

    handlerFactories.stream()
        .flatMap(v -> v.createOutputDatasetFacetBuilders(context).stream())
        .forEach(
            f ->
                f.accept(
                    datasetVersionDatasetFacet,
                    new BiConsumer<String, OutputDatasetFacet>() {
                      @Override
                      public void accept(String s, OutputDatasetFacet outputDatasetFacet) {
                        facetsBuilder.getOutputFacets().put(s, outputDatasetFacet);
                      }
                    }));
  }
}
