/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.fabric;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.FabricLakehouseUtils;
import io.openlineage.client.utils.FabricWarehouseUtils;
import io.openlineage.client.utils.OneLakeLocation;
import io.openlineage.spark.api.DatasetIdentifierResolver;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.Value;

/**
 * Renames tables that Spark reads or writes through their OneLake paths, {@code
 * abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<item>/Tables/[<schema>/]<table>}.
 *
 * <p>A path with IDs doesn't say whether the item is a lakehouse or a warehouse. So a table is
 * renamed only when the path and the session's settings make that clear:
 *
 * <ul>
 *   <li>The item ID is the notebook's default lakehouse ({@code trident.lakehouse.id}). The table
 *       is named after that lakehouse ({@code trident.lakehouse.name}).
 *   <li>The path uses names, such as {@code my-ws@…/lh_sales.Lakehouse/Tables/…}, and the workspace
 *       is the notebook's ({@code trident.workspace.name}). Then the workspace ID is known.
 *   <li>The path uses names and the item ends in {@code .Warehouse} or {@code .Datawarehouse}, in
 *       the notebook's workspace. The table gets the name the {@code synapsesql} connector gives
 *       it: {@code fabric-warehouse://<SQL address>} / {@code <warehouse>.<schema>.<table>}, with
 *       the address built from {@code trident.tenant.id} and the workspace ID.
 * </ul>
 *
 * <p>Lakehouse tables are named as {@link FabricLakehouseUtils} describes. All other paths keep
 * their names.
 */
public class OneLakePathResolver implements DatasetIdentifierResolver {
  private static final String LAKEHOUSE_SUFFIX = "." + FabricLakehouseUtils.LAKEHOUSE;
  private static final List<String> WAREHOUSE_SUFFIXES =
      Arrays.asList("." + OneLakeLocation.WAREHOUSE, ".Datawarehouse");
  private static final int WAREHOUSE_PATH_PARTS = 3;

  private final FabricSession session;

  /**
   * @param session the settings of a session that runs in Fabric
   */
  public OneLakePathResolver(FabricSession session) {
    this.session = session;
  }

  @Override
  public DatasetIdentifier resolve(DatasetIdentifier identifier) {
    Optional<OneLakeLocation> byId =
        OneLakeLocation.parse(identifier.getNamespace(), identifier.getName());
    if (byId.isPresent()) {
      OneLakeLocation location = byId.get();
      if (session.getLakehouseName() == null
          || !location.getWorkspaceId().equals(session.getWorkspaceId())
          || !location.getItemId().equals(session.getLakehouseId())) {
        return identifier;
      }
      return lakehouseTable(identifier, session.getLakehouseName(), location);
    }
    return byName(identifier)
        .map(
            item ->
                item.isWarehouse()
                    ? warehouseTable(identifier, item)
                    : lakehouseTable(identifier, item.getName(), item.getLocation()))
        .orElse(identifier);
  }

  private static DatasetIdentifier lakehouseTable(
      DatasetIdentifier identifier, String lakehouse, OneLakeLocation location) {
    return FabricLakehouseUtils.tableAt(lakehouse, location, identifier.getSymlinks())
        .orElse(identifier);
  }

  /**
   * Names a warehouse table the way the {@code synapsesql} connector does. Warehouses always have
   * schemas, so the path must be {@code Tables/<schema>/<table>}. The warehouse's ID isn't in the
   * path, so there's no {@code LOCATION} symlink.
   */
  private DatasetIdentifier warehouseTable(DatasetIdentifier identifier, NamedItem item) {
    List<String> path = item.getLocation().getPath();
    if (session.getTenantId() == null
        || path.size() != WAREHOUSE_PATH_PARTS
        || !OneLakeLocation.TABLES.equals(path.get(0))) {
      return identifier;
    }
    return new DatasetIdentifier(
        item.getName() + "." + path.get(1) + "." + path.get(2),
        FabricWarehouseUtils.SCHEME
            + "://"
            + FabricWarehouseUtils.endpointFor(session.getTenantId(), session.getWorkspaceId()),
        new ArrayList<>(
            identifier.getSymlinks() == null ? Collections.emptyList() : identifier.getSymlinks()));
  }

  @Value
  private static class NamedItem {
    String name;
    boolean warehouse;
    OneLakeLocation location;
  }

  /**
   * Reads a path that uses names, such as {@code abfss://my-ws@<OneLake
   * host>/lh.Lakehouse/Tables/…}, in the notebook's workspace. A lakehouse's ID is only known for
   * the notebook's default lakehouse.
   */
  private Optional<NamedItem> byName(DatasetIdentifier identifier) {
    if (session.getWorkspaceName() == null) {
      return Optional.empty();
    }
    URI namespace;
    try {
      namespace = new URI(identifier.getNamespace());
    } catch (URISyntaxException e) {
      return Optional.empty();
    }
    // the workspace is the URI's user info, which java.net.URI decodes
    if (!"abfss".equalsIgnoreCase(namespace.getScheme())
        || !OneLakeLocation.isOneLakeHost(namespace.getHost())
        || !session.getWorkspaceName().equals(namespace.getUserInfo())) {
      return Optional.empty();
    }
    List<String> segments =
        Arrays.stream(identifier.getName().split("/"))
            .filter(segment -> !segment.isEmpty())
            .collect(Collectors.toList());
    if (segments.isEmpty()) {
      return Optional.empty();
    }
    String item = segments.get(0);
    Optional<String> warehouse =
        WAREHOUSE_SUFFIXES.stream()
            .map(suffix -> stripSuffix(item, suffix))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .findFirst();
    Optional<String> lakehouse =
        warehouse.isPresent() ? Optional.empty() : stripSuffix(item, LAKEHOUSE_SUFFIX);
    if (!warehouse.isPresent() && !lakehouse.isPresent()) {
      return Optional.empty();
    }
    List<String> path = new ArrayList<>(segments.subList(1, segments.size()));
    if (!path.isEmpty() && OneLakeLocation.TABLES.equalsIgnoreCase(path.get(0))) {
      path.set(0, OneLakeLocation.TABLES);
    }
    String name = warehouse.orElseGet(lakehouse::get);
    UUID lakehouseId = null;
    if (lakehouse.isPresent() && name.equalsIgnoreCase(session.getLakehouseName())) {
      // the default lakehouse: use its ID and its name as the session spells it, so a path with
      // names and a path with IDs give the table the same name
      lakehouseId = session.getLakehouseId();
      name = session.getLakehouseName();
    }
    return Optional.of(
        new NamedItem(
            name,
            warehouse.isPresent(),
            OneLakeLocation.builder()
                .workspaceId(session.getWorkspaceId())
                .itemId(lakehouseId)
                .path(path)
                .build()));
  }

  /** Removes the type suffix, ignoring case; empty if the item doesn't end with it. */
  private static Optional<String> stripSuffix(String item, String suffix) {
    if (item.length() <= suffix.length()
        || !item.toLowerCase(Locale.ROOT).endsWith(suffix.toLowerCase(Locale.ROOT))) {
      return Optional.empty();
    }
    return Optional.of(item.substring(0, item.length() - suffix.length()));
  }
}
