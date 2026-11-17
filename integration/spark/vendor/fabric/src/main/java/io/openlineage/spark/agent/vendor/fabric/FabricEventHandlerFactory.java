/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.fabric;

import io.openlineage.spark.api.DatasetIdentityFacetBuilder;
import io.openlineage.spark.api.FileWriteTargetResolver;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

/**
 * Adds the {@code fabric_onelake} facet to datasets stored in OneLake, and reports the warehouse
 * table as the output of writes with Microsoft's Spark connector for Fabric.
 */
public class FabricEventHandlerFactory implements OpenLineageEventHandlerFactory {

  @Override
  public Collection<DatasetIdentityFacetBuilder> createDatasetIdentityFacetBuilders(
      OpenLineageContext context) {
    return Collections.singletonList(
        (identifier, facets) ->
            OneLakeFacetUtils.facetFor(context, identifier).ifPresent(facets::fabric_onelake));
  }

  @Override
  public Collection<FileWriteTargetResolver> createFileWriteTargetResolvers(
      OpenLineageContext context) {
    return Collections.singletonList(
        command ->
            FabricWarehouseSparkUtils.writeTarget(
                command,
                context
                    .getSparkSession()
                    .map(FabricWarehouseSparkUtils::confOf)
                    .orElse(key -> Optional.empty())));
  }
}
