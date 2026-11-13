/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.FabricWarehouseUtils;
import io.openlineage.client.utils.OneLakeLocation;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Value;

/**
 * Names tables in Microsoft Fabric lakehouses {@code fabric-lakehouse://<workspace ID>} / {@code
 * <lakehouse>.<schema>.<table>}, with schema {@code dbo} in lakehouses without schemas.
 *
 * <p>Spark reads and writes lakehouse tables through their files in OneLake, {@code
 * abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<item>/Tables/[<schema>/]<table>}. Such a
 * path doesn't say whether the item is a lakehouse or a warehouse, so a table is only named as a
 * lakehouse table when its path alone, with the Fabric session's settings, shows it:
 *
 * <ul>
 *   <li>the path's item ID is the session's default lakehouse ({@code trident.lakehouse.id}), named
 *       {@code trident.lakehouse.name};
 *   <li>the path is written with names, {@code <workspace>@…/<name>.Lakehouse/Tables/…}, in the
 *       session's own workspace ({@code trident.workspace.name}), so the workspace ID is known.
 * </ul>
 *
 * The table's OneLake location written with IDs is kept as a {@code LOCATION} symlink when the
 * lakehouse's ID is known.
 *
 * <p>A path written with names that ends in {@code <name>.Warehouse} or {@code
 * <name>.Datawarehouse}, in the session's own workspace, is a warehouse table. It is named as the
 * {@code synapsesql} connector names it, {@code fabric-warehouse://<SQL address>} / {@code
 * <warehouse>.<schema>.<table>}, with the address built from {@code trident.tenant.id} and the
 * workspace ID. Every other path keeps its name.
 */
public final class FabricLakehouseSparkUtils {
  public static final String SCHEME = "fabric-lakehouse";
  public static final String LAKEHOUSE = "Lakehouse";
  public static final String DEFAULT_SCHEMA = "dbo";
  static final String LAKEHOUSE_ID_CONF = "trident.lakehouse.id";
  static final String LAKEHOUSE_NAME_CONF = "trident.lakehouse.name";

  private static final String PREFIX = SCHEME + "://";
  private static final String LAKEHOUSE_SUFFIX = "." + LAKEHOUSE;
  private static final List<String> WAREHOUSE_SUFFIXES =
      Arrays.asList("." + OneLakeLocation.WAREHOUSE, ".Datawarehouse");

  private FabricLakehouseSparkUtils() {}

  /** The Fabric session's settings that lakehouse naming reads. */
  @Value
  public static class Session {
    UUID tenantId;
    UUID workspaceId;
    String workspaceName;
    UUID lakehouseId;
    String lakehouseName;

    /**
     * Reads the settings, or returns empty outside Fabric, where there is no {@code
     * trident.workspace.id}.
     */
    public static Optional<Session> of(Function<String, Optional<String>> conf) {
      return conf.apply(FabricWarehouseSparkUtils.WORKSPACE_ID_CONF)
          .flatMap(FabricWarehouseUtils::parseGuid)
          .map(
              workspaceId ->
                  new Session(
                      conf.apply(FabricWarehouseSparkUtils.TENANT_ID_CONF)
                          .flatMap(FabricWarehouseUtils::parseGuid)
                          .orElse(null),
                      workspaceId,
                      nonBlank(conf.apply(OneLakeFacetUtils.WORKSPACE_NAME_CONF)),
                      conf.apply(LAKEHOUSE_ID_CONF)
                          .flatMap(FabricWarehouseUtils::parseGuid)
                          .orElse(null),
                      nonBlank(conf.apply(LAKEHOUSE_NAME_CONF))));
    }

    private static String nonBlank(Optional<String> value) {
      return value.map(String::trim).filter(v -> !v.isEmpty()).orElse(null);
    }
  }

  /**
   * Names the dataset as a lakehouse table if its path shows it is one (see this class's
   * description), or returns it unchanged.
   */
  public static DatasetIdentifier resolve(DatasetIdentifier identifier, Session session) {
    Optional<OneLakeLocation> byId =
        OneLakeLocation.parse(identifier.getNamespace(), identifier.getName());
    if (byId.isPresent()) {
      OneLakeLocation location = byId.get();
      if (session.getLakehouseName() == null
          || !location.getWorkspaceId().equals(session.getWorkspaceId())
          || !location.getItemId().equals(session.getLakehouseId())) {
        return identifier;
      }
      return named(identifier, session.getLakehouseName(), location);
    }
    return byName(identifier, session)
        .map(
            item ->
                item.isWarehouse()
                    ? warehouseTable(identifier, item, session)
                    : named(identifier, item.getName(), item.getLocation()))
        .orElse(identifier);
  }

  /**
   * Describes a lakehouse table named by {@link #resolve} in OneLake terms, for the {@code
   * fabric_onelake} facet: the workspace from the namespace, the lakehouse from the name, and the
   * lakehouse's ID and the table's location from the {@code LOCATION} symlink.
   *
   * @return the description, or empty if this isn't a {@code fabric-lakehouse} dataset
   */
  public static Optional<OneLakeLocation> toOneLakeLocation(DatasetIdentifier identifier) {
    String[] parts = identifier.getName().split("\\.");
    if (!identifier.getNamespace().startsWith(PREFIX) || parts.length != 3) {
      return Optional.empty();
    }
    Optional<UUID> workspaceId =
        FabricWarehouseUtils.parseGuid(identifier.getNamespace().substring(PREFIX.length()));
    OneLakeLocation.OneLakeLocationBuilder builder =
        OneLakeLocation.builder()
            .workspaceId(workspaceId.orElse(null))
            .itemName(parts[0])
            .itemType(LAKEHOUSE);
    OneLakeLocation.locationSymlink(identifier, workspaceId.orElse(null))
        .ifPresent(location -> builder.itemId(location.getItemId()).path(location.getPath()));
    return Optional.of(builder.build());
  }

  private static DatasetIdentifier named(
      DatasetIdentifier identifier, String lakehouse, OneLakeLocation location) {
    List<String> path = location.getPath();
    // Tables/<table> in a lakehouse without schemas, Tables/<schema>/<table> otherwise
    if (path.size() < 2 || path.size() > 3 || !OneLakeLocation.TABLES.equals(path.get(0))) {
      return identifier;
    }
    String schema = path.size() == 3 ? path.get(1) : DEFAULT_SCHEMA;
    String table = path.get(path.size() - 1);
    DatasetIdentifier named =
        new DatasetIdentifier(
            lakehouse + "." + schema + "." + table,
            PREFIX + location.getWorkspaceId(),
            new ArrayList<>(
                identifier.getSymlinks() == null
                    ? Collections.emptyList()
                    : identifier.getSymlinks()));
    location
        .guidIdentifier()
        .ifPresent(
            guid ->
                named.withSymlink(
                    guid.getName(), guid.getNamespace(), DatasetIdentifier.SymlinkType.LOCATION));
    return named;
  }

  /**
   * Names a warehouse table reached by a path written with names, as the {@code synapsesql}
   * connector does. Warehouses always have schemas, so the path must be {@code
   * Tables/<schema>/<table>}. The warehouse's ID isn't known, so no {@code LOCATION} symlink is
   * added.
   */
  private static DatasetIdentifier warehouseTable(
      DatasetIdentifier identifier, NamedItem item, Session session) {
    List<String> path = item.getLocation().getPath();
    if (session.getTenantId() == null
        || path.size() != 3
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
   * Reads a path written with names, {@code abfss://<workspace>@<OneLake host>} and {@code
   * <name>.Lakehouse/Tables/…} or {@code <name>.Warehouse/Tables/…}, in the session's own
   * workspace. A lakehouse's ID is known only when it is the session's default lakehouse.
   */
  private static Optional<NamedItem> byName(DatasetIdentifier identifier, Session session) {
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
    boolean warehouse;
    String name;
    Optional<String> warehouseName =
        WAREHOUSE_SUFFIXES.stream()
            .map(suffix -> stripSuffix(item, suffix))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .findFirst();
    if (warehouseName.isPresent()) {
      warehouse = true;
      name = warehouseName.get();
    } else {
      Optional<String> lakehouse = stripSuffix(item, LAKEHOUSE_SUFFIX);
      if (!lakehouse.isPresent()) {
        return Optional.empty();
      }
      warehouse = false;
      name = lakehouse.get();
    }
    List<String> path = new ArrayList<>(segments.subList(1, segments.size()));
    if (!path.isEmpty() && OneLakeLocation.TABLES.equalsIgnoreCase(path.get(0))) {
      path.set(0, OneLakeLocation.TABLES);
    }
    UUID lakehouseId =
        !warehouse && name.equalsIgnoreCase(session.getLakehouseName())
            ? session.getLakehouseId()
            : null;
    return Optional.of(
        new NamedItem(
            name,
            warehouse,
            OneLakeLocation.builder()
                .workspaceId(session.getWorkspaceId())
                .itemId(lakehouseId)
                .path(path)
                .build()));
  }

  /** The item's name without its type suffix, matched in any case; empty if it doesn't end so. */
  private static Optional<String> stripSuffix(String item, String suffix) {
    if (item.length() <= suffix.length()
        || !item.toLowerCase(Locale.ROOT).endsWith(suffix.toLowerCase(Locale.ROOT))) {
      return Optional.empty();
    }
    return Optional.of(item.substring(0, item.length() - suffix.length()));
  }
}
