/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils;

import io.openlineage.client.OpenLineage;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

/**
 * What is known about a location in OneLake, Microsoft Fabric's storage, and the {@code oneLake}
 * dataset facet built from it.
 *
 * <p>OneLake paths have the form {@code
 * abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<item>/ <section>/...}, where the section is
 * {@code Tables} or {@code Files}. The workspace and the item can each be written in two ways:
 *
 * <ul>
 *   <li>by ID: {@code
 *       d1f6473b-...@onelake.dfs.fabric.microsoft.com/c7a14054-.../Tables/sales/orders}
 *   <li>by name, with the item's type after a dot: {@code
 *       sales-ws@onelake.dfs.fabric.microsoft.com/lh_sales.Lakehouse/Tables/sales/orders}
 * </ul>
 *
 * OneLake refuses paths that mix the two. Every field is optional: each producer sets only what it
 * knows locally, without calling Fabric.
 */
@Value
@Builder(toBuilder = true)
public class OneLakeLocation {
  public static final String HOST = "onelake.dfs.fabric.microsoft.com";
  public static final String LAKEHOUSE = "Lakehouse";
  public static final String WAREHOUSE = "Warehouse";
  public static final String TABLES = "Tables";
  public static final String FILES = "Files";
  private static final String SPARK_WAREHOUSE_SUFFIX = "Datawarehouse";

  // The global host, the blob host and regional hosts such as westus-onelake.dfs.fabric...
  private static final Pattern HOST_PATTERN =
      Pattern.compile("^(?:[a-z0-9]+-)?onelake\\.(?:dfs|blob)\\.fabric\\.microsoft\\.com$");
  private static final Pattern GUID =
      Pattern.compile(
          "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");

  UUID workspaceId;
  String workspaceName;
  UUID itemId;
  String itemName;

  /** Fabric's name for the item's type, such as {@code Lakehouse} or {@code Warehouse}. */
  String itemType;

  /** Path segments after the item, such as {@code [Tables, sales, orders]}. */
  @NonNull @Builder.Default List<String> path = Collections.emptyList();

  /** ID of the table's Delta metadata ({@code metaData.id}). */
  String tableId;

  /**
   * Reads a OneLake location from a dataset's namespace and name, such as {@code
   * abfss://<workspace>@onelake.dfs.fabric.microsoft.com} and {@code <item>/Tables/<table>}.
   *
   * @return the location, or empty if this isn't a OneLake path that names an item
   */
  public static Optional<OneLakeLocation> parse(String namespace, String name) {
    if (namespace == null || name == null) {
      return Optional.empty();
    }
    return parse(namespace + "/" + name);
  }

  /**
   * Reads a OneLake location from a full path, such as {@code
   * abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<item>/Tables/<table>}.
   *
   * @return the location, or empty if this isn't a OneLake path that names an item
   */
  public static Optional<OneLakeLocation> parse(String location) {
    if (location == null) {
      return Optional.empty();
    }
    int schemeEnd = location.indexOf("://");
    if (schemeEnd < 0) {
      return Optional.empty();
    }
    String scheme = location.substring(0, schemeEnd).toLowerCase(Locale.ROOT);
    if (!"abfss".equals(scheme) && !"abfs".equals(scheme)) {
      return Optional.empty();
    }
    String rest = location.substring(schemeEnd + 3);
    int slash = rest.indexOf('/');
    String authority = slash < 0 ? rest : rest.substring(0, slash);
    String pathPart = slash < 0 ? "" : rest.substring(slash + 1);
    int at = authority.lastIndexOf('@');
    if (at <= 0) {
      return Optional.empty();
    }
    String container = authority.substring(0, at);
    String host = authority.substring(at + 1).toLowerCase(Locale.ROOT);
    if (!HOST_PATTERN.matcher(host).matches()) {
      return Optional.empty();
    }

    List<String> segments =
        Arrays.stream(pathPart.split("/")).filter(s -> !s.isEmpty()).collect(Collectors.toList());
    if (segments.isEmpty()) {
      return Optional.empty();
    }

    OneLakeLocationBuilder builder = OneLakeLocation.builder();
    Optional<UUID> workspaceGuid = guid(container);
    if (workspaceGuid.isPresent()) {
      builder.workspaceId(workspaceGuid.get());
    } else {
      builder.workspaceName(container);
    }

    String item = segments.get(0);
    Optional<UUID> itemGuid = guid(item);
    if (itemGuid.isPresent()) {
      builder.itemId(itemGuid.get());
    } else {
      int dot = item.lastIndexOf('.');
      if (dot <= 0 || dot == item.length() - 1) {
        // OneLake rejects a bare item name; this isn't a path it would accept
        return Optional.empty();
      }
      builder.itemName(item.substring(0, dot)).itemType(itemType(item.substring(dot + 1)));
    }
    if (workspaceGuid.isPresent() != itemGuid.isPresent()) {
      // OneLake rejects paths that mix IDs and names
      return Optional.empty();
    }

    List<String> path = new ArrayList<>(segments.subList(1, segments.size()));
    if (!path.isEmpty()) {
      path.set(0, section(path.get(0)));
    }
    return Optional.of(builder.path(Collections.unmodifiableList(path)).build());
  }

  /** Whether this host is a OneLake host, such as {@code onelake.dfs.fabric.microsoft.com}. */
  public static boolean isOneLakeHost(String host) {
    return host != null && HOST_PATTERN.matcher(host.toLowerCase(Locale.ROOT)).matches();
  }

  /** Reads an ID, returning empty instead of failing when the text isn't one. */
  public static Optional<UUID> guid(String value) {
    if (value == null || !GUID.matcher(value.trim()).matches()) {
      return Optional.empty();
    }
    return Optional.of(UUID.fromString(value.trim()));
  }

  /**
   * Fabric's name for an item type written as a path suffix. Spark sometimes reports warehouses as
   * {@code .Datawarehouse}, while OneLake's own APIs use {@code .Warehouse}.
   */
  public static String itemType(String suffix) {
    if (LAKEHOUSE.equalsIgnoreCase(suffix)) {
      return LAKEHOUSE;
    }
    if (WAREHOUSE.equalsIgnoreCase(suffix) || SPARK_WAREHOUSE_SUFFIX.equalsIgnoreCase(suffix)) {
      return WAREHOUSE;
    }
    return suffix;
  }

  /** The table's segments after {@code Tables/}: {@code [schema, table]} or {@code [table]}. */
  public Optional<List<String>> tableParts() {
    if (path.size() < 2 || path.size() > 3 || !TABLES.equals(path.get(0))) {
      return Optional.empty();
    }
    return Optional.of(path.subList(1, path.size()));
  }

  /**
   * The location written with IDs, as {@code abfss://<workspace
   * ID>@onelake.dfs.fabric.microsoft.com/<item ID>/...}, with lowercase IDs and no trailing slash.
   *
   * @return the location, or empty if either ID isn't known or there's no path after the item, as
   *     the item's root isn't a dataset
   */
  public Optional<DatasetIdentifier> guidIdentifier() {
    if (workspaceId == null || itemId == null || path.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(identifier(workspaceId.toString(), itemId.toString()));
  }

  /**
   * The location written the standard way: with IDs if both are known, otherwise with names. The
   * host is always {@code onelake.dfs.fabric.microsoft.com}, IDs are lowercase and item types use
   * Fabric's names.
   *
   * @return the identifier, or empty if neither form can be written or there's no path after the
   *     item
   */
  public Optional<DatasetIdentifier> canonicalIdentifier() {
    Optional<DatasetIdentifier> byId = guidIdentifier();
    if (byId.isPresent()) {
      return byId;
    }
    if (workspaceName == null || itemName == null || itemType == null || path.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(identifier(workspaceName, itemName + "." + itemType));
  }

  /** {@link #guidIdentifier()} as a single path. */
  public Optional<String> guidLocation() {
    return guidIdentifier().map(id -> id.getNamespace() + "/" + id.getName());
  }

  /**
   * Builds the {@code oneLake} facet from everything known. The table ID is written in lowercase,
   * as the spec requires.
   */
  public OpenLineage.OneLakeDatasetFacet toFacet(OpenLineage openLineage) {
    return openLineage
        .newOneLakeDatasetFacetBuilder()
        .location(guidLocation().orElse(null))
        .workspaceId(workspaceId == null ? null : workspaceId.toString())
        .workspaceName(workspaceName)
        .itemId(itemId == null ? null : itemId.toString())
        .itemName(itemName)
        .itemType(itemType)
        .tableId(tableId == null ? null : tableId.toLowerCase(Locale.ROOT))
        .build();
  }

  private DatasetIdentifier identifier(String container, String item) {
    StringBuilder name = new StringBuilder(item);
    for (String segment : path) {
      name.append('/').append(segment);
    }
    return new DatasetIdentifier(name.toString(), "abfss://" + container + "@" + HOST);
  }

  private static String section(String segment) {
    if (TABLES.equalsIgnoreCase(segment)) {
      return TABLES;
    }
    if (FILES.equalsIgnoreCase(segment)) {
      return FILES;
    }
    return segment;
  }
}
