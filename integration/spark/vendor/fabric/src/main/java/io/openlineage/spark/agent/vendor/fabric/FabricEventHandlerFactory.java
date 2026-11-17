/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.fabric;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.DatasetIdentifierResolver;
import io.openlineage.spark.api.DatasetIdentityFacetBuilder;
import io.openlineage.spark.api.FileWriteTargetResolver;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/**
 * Adds the {@code fabric_onelake} facet to datasets stored in OneLake, and reports the warehouse
 * table as the output of writes with Microsoft's Spark connector for Fabric. In Fabric sessions, it
 * also names tables reached through their OneLake paths (see {@link OneLakePathResolver}) and adds
 * the {@code fabric_spark} run facet to every event.
 */
@Slf4j
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

  @Override
  public Collection<DatasetIdentifierResolver> createDatasetIdentifierResolvers(
      OpenLineageContext context) {
    return session(context)
        .filter(FabricSession::isFabric)
        .<Collection<DatasetIdentifierResolver>>map(
            session -> Collections.singletonList(new OneLakePathResolver(session)))
        .orElse(Collections.emptyList());
  }

  @Override
  public Collection<CustomFacetBuilder<?, ? extends OpenLineage.RunFacet>> createRunFacetBuilders(
      OpenLineageContext context) {
    return Collections.singletonList(new FabricSparkRunFacetBuilder(context));
  }

  // This runs for every Spark job, so failing to read the settings must not break lineage for
  // jobs outside Fabric; the datasets just keep their names.
  @SuppressWarnings("PMD.AvoidCatchingGenericException")
  private static Optional<FabricSession> session(OpenLineageContext context) {
    try {
      return context
          .getSparkSession()
          .map(FabricWarehouseSparkUtils::confOf)
          .map(FabricSession::of);
    } catch (RuntimeException e) {
      log.warn("Cannot read the Fabric session settings; OneLake paths keep their names", e);
      return Optional.empty();
    }
  }
}
