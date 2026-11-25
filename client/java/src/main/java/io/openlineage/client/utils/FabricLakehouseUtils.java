/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Names Microsoft Fabric lakehouse tables as the naming conventions describe: namespace {@code
 * fabric-lakehouse://<workspace ID>}, name {@code <lakehouse>.<schema>.<table>}. Lakehouses without
 * schemas use the schema {@code dbo}.
 *
 * <p>A lakehouse table's files are in OneLake, at {@code
 * abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>/Tables/[<schema>/]<table>}. That
 * path doesn't say whether the item is a lakehouse, so the caller has to know. This class only
 * converts between a lakehouse table's location and its name.
 */
public final class FabricLakehouseUtils {
  public static final String SCHEME = "fabric-lakehouse";
  public static final String LAKEHOUSE = "Lakehouse";
  public static final String DEFAULT_SCHEMA = "dbo";

  private static final String PREFIX = SCHEME + "://";
  private static final int NAME_PARTS = 3;

  private FabricLakehouseUtils() {}

  /**
   * Names the table at a location in a lakehouse. If the lakehouse's ID is known, the table's
   * OneLake path is added as a {@code LOCATION} symlink.
   *
   * @param lakehouse the lakehouse's name
   * @param location the table's location: the workspace ID and {@code Tables/<table>} or {@code
   *     Tables/<schema>/<table>}, with the lakehouse's ID when it's known
   * @param symlinks symlinks to keep, or null
   * @return the table, or empty if the location isn't a table in a lakehouse
   */
  public static Optional<DatasetIdentifier> tableAt(
      String lakehouse, OneLakeLocation location, List<DatasetIdentifier.Symlink> symlinks) {
    List<String> path = location.getPath();
    // Tables/<table> in a lakehouse without schemas, Tables/<schema>/<table> otherwise
    if (location.getWorkspaceId() == null
        || path == null
        || path.size() < 2
        || path.size() > NAME_PARTS
        || !OneLakeLocation.TABLES.equals(path.get(0))) {
      return Optional.empty();
    }
    String schema = path.size() == NAME_PARTS ? path.get(1) : DEFAULT_SCHEMA;
    String table = path.get(path.size() - 1);
    DatasetIdentifier named =
        new DatasetIdentifier(
            lakehouse + "." + schema + "." + table,
            PREFIX + location.getWorkspaceId(),
            new ArrayList<>(symlinks == null ? Collections.emptyList() : symlinks));
    location
        .guidIdentifier()
        .ifPresent(
            guid ->
                named.withSymlink(
                    guid.getName(), guid.getNamespace(), DatasetIdentifier.SymlinkType.LOCATION));
    return Optional.of(named);
  }

  /**
   * Describes where a lakehouse table is stored, for the {@code fabric_onelake} facet. The
   * workspace comes from the namespace, the lakehouse name from the table name, and the lakehouse's
   * ID and the table's path from the {@code LOCATION} symlink.
   *
   * @return the description, or empty if this isn't a {@code fabric-lakehouse} dataset named {@code
   *     <lakehouse>.<schema>.<table>}
   */
  public static Optional<OneLakeLocation> toOneLakeLocation(DatasetIdentifier identifier) {
    String[] parts = identifier.getName().split("\\.");
    if (!identifier.getNamespace().startsWith(PREFIX) || parts.length != NAME_PARTS) {
      return Optional.empty();
    }
    Optional<UUID> workspaceId =
        OneLakeLocation.guid(identifier.getNamespace().substring(PREFIX.length()));
    OneLakeLocation.OneLakeLocationBuilder builder =
        OneLakeLocation.builder()
            .workspaceId(workspaceId.orElse(null))
            .itemName(parts[0])
            .itemType(LAKEHOUSE);
    OneLakeLocation.locationSymlink(identifier, workspaceId.orElse(null))
        .ifPresent(location -> builder.itemId(location.getItemId()).path(location.getPath()));
    return Optional.of(builder.build());
  }
}
