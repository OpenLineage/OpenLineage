/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.fabric;

import io.openlineage.spark.agent.util.FabricLakehouseSparkUtils;
import io.openlineage.spark.agent.util.FabricWarehouseSparkUtils;
import io.openlineage.spark.api.DatasetIdentifierResolver;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/** Names datasets from their OneLake paths, in Fabric sessions only. */
@Slf4j
public class FabricEventHandlerFactory implements OpenLineageEventHandlerFactory {

  @Override
  public Collection<DatasetIdentifierResolver> createDatasetIdentifierResolvers(
      OpenLineageContext context) {
    return session(context)
        .<Collection<DatasetIdentifierResolver>>map(
            session ->
                Collections.singletonList(
                    identifier -> FabricLakehouseSparkUtils.resolve(identifier, session)))
        .orElse(Collections.emptyList());
  }

  // This runs for every Spark job, so failing to read the settings must not break lineage for
  // jobs outside Fabric; the datasets just keep their names.
  @SuppressWarnings("PMD.AvoidCatchingGenericException")
  private static Optional<FabricLakehouseSparkUtils.Session> session(OpenLineageContext context) {
    try {
      return context
          .getSparkSession()
          .map(FabricWarehouseSparkUtils::confOf)
          .flatMap(FabricLakehouseSparkUtils.Session::of);
    } catch (RuntimeException e) {
      log.warn("Cannot read the Fabric session settings; OneLake paths keep their names", e);
      return Optional.empty();
    }
  }
}
