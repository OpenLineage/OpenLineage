/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.dataset.DatasetCompositeFacetsBuilder;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Adds the facets that follow from a dataset's identity: its final name, namespace and symlinks.
 */
public final class DatasetIdentityFacetUtils {
  private DatasetIdentityFacetUtils() {}

  /**
   * Adds every facet derived from the dataset's identity, such as {@code fabric_onelake} for
   * datasets stored in Microsoft Fabric's OneLake. The symlinks are the ones in the dataset's
   * {@code symlinks} facet, however it was set, so the identity facets always describe the symlinks
   * the event reports.
   */
  public static void buildIdentityFacets(
      OpenLineageContext context,
      DatasetCompositeFacetsBuilder facetsBuilder,
      String name,
      String namespace) {
    DatasetIdentifier identifier =
        new DatasetIdentifier(
            name, namespace, symlinksOf(facetsBuilder.getFacets().build().getSymlinks()));
    OneLakeFacetUtils.facetFor(context, identifier)
        .ifPresent(facet -> facetsBuilder.getFacets().fabric_onelake(facet));
  }

  /** The facet's symlinks; those of a type {@link DatasetIdentifier} doesn't know are skipped. */
  private static List<DatasetIdentifier.Symlink> symlinksOf(
      OpenLineage.SymlinksDatasetFacet facet) {
    if (facet == null || facet.getIdentifiers() == null) {
      return Collections.emptyList();
    }
    return facet.getIdentifiers().stream()
        .map(
            identifier ->
                symlinkType(identifier.getType())
                    .map(
                        type ->
                            new DatasetIdentifier.Symlink(
                                identifier.getName(), identifier.getNamespace(), type)))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toList());
  }

  private static Optional<DatasetIdentifier.SymlinkType> symlinkType(String type) {
    if (type == null) {
      return Optional.empty();
    }
    String upper = type.toUpperCase(Locale.ROOT);
    return Arrays.stream(DatasetIdentifier.SymlinkType.values())
        .filter(t -> t.name().equals(upper))
        .findFirst();
  }
}
