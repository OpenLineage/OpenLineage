/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.fabric;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.FabricLakehouseUtils;
import io.openlineage.client.utils.FabricWarehouseUtils;
import io.openlineage.client.utils.OneLakeLocation;
import io.openlineage.spark.agent.util.FacetUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Optional;

/**
 * Adds the {@code fabric_onelake} facet to datasets stored in OneLake, Microsoft Fabric's storage:
 * Fabric warehouse and lakehouse tables, and OneLake paths that use IDs.
 *
 * <p>The facet is built from the dataset's own name, namespace and symlinks, so it describes the
 * dataset however the job reached it, including from outside Fabric. When the job runs in Fabric,
 * the session's settings can add the workspace's name, but only for the workspace the session runs
 * in: a job can read and write items in other workspaces.
 */
public final class OneLakeFacetUtils {
  public static final String FACET_NAME = "fabric_onelake";

  private OneLakeFacetUtils() {}

  /**
   * Builds the {@code fabric_onelake} facet of a dataset.
   *
   * @param context the OpenLineage context, for the session's settings and the disabled facets
   * @param identifier the dataset, with its symlinks
   * @return the facet, or empty if the dataset isn't stored in OneLake or the facet is disabled
   */
  public static Optional<OpenLineage.FabricOneLakeDatasetFacet> facetFor(
      OpenLineageContext context, DatasetIdentifier identifier) {
    if (FacetUtils.isFacetDisabled(context, FACET_NAME)) {
      return Optional.empty();
    }
    return describe(identifier)
        .map(
            location ->
                withWorkspaceName(
                    location,
                    FabricSession.of(
                        context
                            .getSparkSession()
                            .map(FabricWarehouseSparkUtils::confOf)
                            .orElse(key -> Optional.empty()))))
        .map(location -> location.toFacet(context.getOpenLineage()));
  }

  /**
   * Describes where the dataset is stored in OneLake. For a warehouse or lakehouse table, this
   * comes from its name and {@code LOCATION} symlink; for any other OneLake path with IDs, from the
   * path.
   */
  private static Optional<OneLakeLocation> describe(DatasetIdentifier identifier) {
    Optional<OneLakeLocation> location = FabricWarehouseUtils.toOneLakeLocation(identifier);
    if (!location.isPresent()) {
      location = FabricLakehouseUtils.toOneLakeLocation(identifier);
    }
    if (!location.isPresent()) {
      location = OneLakeLocation.parse(identifier.getNamespace(), identifier.getName());
    }
    return location;
  }

  /**
   * Adds the workspace's name from {@code trident.workspace.name}, if the location is in the
   * workspace the Fabric session runs in.
   */
  static OneLakeLocation withWorkspaceName(OneLakeLocation location, FabricSession session) {
    if (location.getWorkspaceName() != null
        || location.getWorkspaceId() == null
        || session.getWorkspaceName() == null
        || !location.getWorkspaceId().equals(session.getWorkspaceId())) {
      return location;
    }
    return location.toBuilder().workspaceName(session.getWorkspaceName()).build();
  }
}
