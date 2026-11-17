/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.dataset.DatasetCompositeFacetsBuilder;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.api.DatasetIdentityFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * Adds the facets that follow from a dataset's identity: its final name, namespace and symlinks.
 */
@Slf4j
public final class DatasetIdentityFacetUtils {
  private DatasetIdentityFacetUtils() {}

  /**
   * Runs the vendors' {@link io.openlineage.spark.api.DatasetIdentityFacetBuilder}s on the dataset.
   * They get the symlinks from the dataset's {@code symlinks} facet, so what they add always
   * matches the symlinks the event reports.
   */
  public static void buildIdentityFacets(
      OpenLineageContext context,
      DatasetCompositeFacetsBuilder facetsBuilder,
      String name,
      String namespace) {
    DatasetIdentifier identifier =
        new DatasetIdentifier(
            name, namespace, symlinksOf(facetsBuilder.getFacets().build().getSymlinks()));
    for (DatasetIdentityFacetBuilder builder : context.getDatasetIdentityFacetBuilders()) {
      build(builder, identifier, facetsBuilder);
    }
  }

  // A vendor's builder runs for every dataset. If it fails, only its facets are missing; the
  // dataset is still reported. LinkageError is caught too: vendor modules are compiled against
  // one Spark version and can meet another at runtime.
  @SuppressWarnings("PMD.AvoidCatchingGenericException")
  private static void build(
      DatasetIdentityFacetBuilder builder,
      DatasetIdentifier identifier,
      DatasetCompositeFacetsBuilder facetsBuilder) {
    try {
      builder.build(identifier, facetsBuilder.getFacets());
    } catch (Exception | LinkageError e) {
      log.warn("Dataset facet builder {} failed; leaving its facets out", builder.getClass(), e);
    }
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
