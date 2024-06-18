/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.dataset.namespace.resolver;

import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.dataset.DatasetConfig;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class DatasetNamespaceResolverLoader {

  private static List<DatasetNamespaceResolverBuilder> builders =
      Arrays.asList(
          new HostListNamespaceResolverBuilder(),
          new PatternNamespaceResolverBuilder(),
          new PatternMatchingGroupNamespaceResolverBuilder());

  public static Class<? extends DatasetNamespaceResolverConfig>
      loadDatasetNamespaceResolverConfigByType(String type) {
    return getDatasetNamespaceResolverBuilder(b -> b.getType().equalsIgnoreCase(type))
        .map(b -> b.getConfig().getClass())
        .orElseThrow(
            () ->
                new OpenLineageClientException(
                    "Invalid dataset namespace resolver type provided: " + type));
  }

  public static String loadDatasetNamespaceResolverTypeByConfigClass(
      Class datasetNamespaceResolverConfigClass) {
    return getDatasetNamespaceResolverBuilder(
            b -> b.getConfig().getClass().equals(datasetNamespaceResolverConfigClass))
        .map(b -> b.getType())
        .orElseThrow(
            () ->
                new OpenLineageClientException(
                    "Invalid dataset namespace resolver class provided: "
                        + datasetNamespaceResolverConfigClass.getCanonicalName()));
  }

  public static List<DatasetNamespaceResolver> loadDatasetNamespaceResolvers(
      DatasetConfig datasetConfig) {
    if (datasetConfig.getNamespaceResolvers() == null) {
      return Collections.emptyList();
    }
    return datasetConfig.getNamespaceResolvers().keySet().stream()
        .map(
            name -> {
              DatasetNamespaceResolverConfig config =
                  datasetConfig.getNamespaceResolvers().get(name);
              return getDatasetNamespaceResolverBuilder(
                      b -> b.getConfig().getClass().equals(config.getClass()))
                  .map(b -> b.build(name, config))
                  .orElseThrow(
                      () ->
                          new OpenLineageClientException(
                              "Dataset namespace resolver shouldn't be called for invalid or null config"));
            })
        .collect(Collectors.toList());
  }

  private static Optional<DatasetNamespaceResolverBuilder> getDatasetNamespaceResolverBuilder(
      Predicate<DatasetNamespaceResolverBuilder> predicate) {

    return Stream.concat(
            builders.stream(),
            StreamSupport.stream(DatasetNamespaceResolverServiceLoader.load().spliterator(), false))
        .filter(predicate)
        .findFirst();
  }

  static class DatasetNamespaceResolverServiceLoader {
    static ServiceLoader<DatasetNamespaceResolverBuilder> load() {
      return ServiceLoader.load(DatasetNamespaceResolverBuilder.class);
    }
  }
}
