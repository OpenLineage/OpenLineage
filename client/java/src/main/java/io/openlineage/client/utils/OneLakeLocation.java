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
 * What is known about a location in OneLake, Microsoft Fabric's storage, and the {@code
 * fabric_onelake} dataset facet built from it.
 *
 * <p>OneLake paths have the form {@code
 * abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<item>/<section>/...}, where the section is
 * {@code Tables} or {@code Files}. This class reads the form written with IDs, {@code
 * d1f6473b-...@onelake.dfs.fabric.microsoft.com/c7a14054-.../Tables/sales/orders}. Every field is
 * optional: each producer sets only what it knows locally, without calling Fabric.
 */
@Value
@Builder(toBuilder = true)
public class OneLakeLocation {
  public static final String HOST = "onelake.dfs.fabric.microsoft.com";
  public static final String WAREHOUSE = "Warehouse";
  public static final String TABLES = "Tables";

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
   * Reads a OneLake location written with IDs from a dataset's namespace and name, such as {@code
   * abfss://<workspace ID>@onelake.dfs.fabric.microsoft.com} and {@code <item ID>/Tables/<table>}.
   *
   * @return the location, or empty if this isn't a OneLake path that names an item by ID
   */
  public static Optional<OneLakeLocation> parse(String namespace, String name) {
    if (namespace == null || name == null) {
      return Optional.empty();
    }
    return parse(namespace + "/" + name);
  }

  /** {@link #parse(String, String)} for a full path. */
  static Optional<OneLakeLocation> parse(String location) {
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

    Optional<UUID> workspaceId = guid(container);
    Optional<UUID> itemId = guid(segments.get(0));
    if (!workspaceId.isPresent() || !itemId.isPresent()) {
      return Optional.empty();
    }

    List<String> path = new ArrayList<>(segments.subList(1, segments.size()));
    if (!path.isEmpty() && TABLES.equalsIgnoreCase(path.get(0))) {
      path.set(0, TABLES);
    }
    return Optional.of(
        OneLakeLocation.builder()
            .workspaceId(workspaceId.get())
            .itemId(itemId.get())
            .path(Collections.unmodifiableList(path))
            .build());
  }

  /** Reads an ID, returning empty instead of failing when the text isn't one. */
  public static Optional<UUID> guid(String value) {
    if (value == null || !GUID.matcher(value.trim()).matches()) {
      return Optional.empty();
    }
    return Optional.of(UUID.fromString(value.trim()));
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
    StringBuilder name = new StringBuilder(itemId.toString());
    for (String segment : path) {
      name.append('/').append(segment);
    }
    return Optional.of(
        new DatasetIdentifier(name.toString(), "abfss://" + workspaceId + "@" + HOST));
  }

  /** {@link #guidIdentifier()} as a single path. */
  public Optional<String> guidLocation() {
    return guidIdentifier().map(id -> id.getNamespace() + "/" + id.getName());
  }

  /**
   * Builds the {@code fabric_onelake} facet from everything known. The table ID is written in
   * lowercase, as the spec requires.
   */
  public OpenLineage.FabricOneLakeDatasetFacet toFacet(OpenLineage openLineage) {
    return openLineage
        .newFabricOneLakeDatasetFacetBuilder()
        .location(guidLocation().orElse(null))
        .workspaceId(workspaceId == null ? null : workspaceId.toString())
        .workspaceName(workspaceName)
        .itemId(itemId == null ? null : itemId.toString())
        .itemName(itemName)
        .itemType(itemType)
        .tableId(tableId == null ? null : tableId.toLowerCase(Locale.ROOT))
        .build();
  }
}
