/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.dataset;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.openlineage.client.MergeConfig;
import io.openlineage.client.dataset.namespace.resolver.DatasetNamespaceResolverConfig;
import io.openlineage.client.dataset.partition.trimmer.DatasetNameTrimmer;
import io.openlineage.client.dataset.partition.trimmer.DateTrimmer;
import io.openlineage.client.dataset.partition.trimmer.KeyValueTrimmer;
import io.openlineage.client.dataset.partition.trimmer.MultiDirDateTrimmer;
import io.openlineage.client.dataset.partition.trimmer.YearMonthTrimmer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@NoArgsConstructor
@AllArgsConstructor
@ToString
@Slf4j
public class DatasetConfig implements MergeConfig<DatasetConfig> {

  private static final List<DatasetNameTrimmer> DEFAULT_DATASET_NAME_TRIMMERS =
      Arrays.asList(
          new DateTrimmer(),
          new KeyValueTrimmer(),
          new MultiDirDateTrimmer(),
          new YearMonthTrimmer());

  @Getter
  @Setter
  @JsonProperty("namespaceResolvers")
  private Map<String, DatasetNamespaceResolverConfig> namespaceResolvers;

  // Semicolon separated list of trimmers classes to disable
  @Setter
  @Getter
  @JsonProperty("disabledTrimmers")
  private String disabledTrimmers;

  // Semicolon separated list of trimmers classes to enable
  @Setter
  @Getter
  @JsonProperty("extraTrimmers")
  private String extraTrimmers;

  @Override
  public DatasetConfig mergeWithNonNull(DatasetConfig other) {
    return new DatasetConfig(
        mergePropertyWith(namespaceResolvers, other.namespaceResolvers),
        mergePropertyWith(disabledTrimmers, other.disabledTrimmers),
        mergePropertyWith(extraTrimmers, other.extraTrimmers));
  }

  public Collection<DatasetNameTrimmer> getDatasetNameTrimmers() {
    Set<DatasetNameTrimmer> trimmers;
    if (this.extraTrimmers != null) {
      trimmers =
          Arrays.stream(this.extraTrimmers.split(";"))
              .map(this::initializeTrimmerByClassName)
              .filter(Optional::isPresent)
              .map(Optional::get)
              .collect(Collectors.toSet());
    } else {
      trimmers = new HashSet<>();
    }

    trimmers.addAll(DEFAULT_DATASET_NAME_TRIMMERS);
    return trimmers.stream()
        .filter(t -> disabledTrimmers == null || !disabledTrimmers.contains(t.getClass().getName()))
        .collect(Collectors.toList());
  }

  private Optional<DatasetNameTrimmer> initializeTrimmerByClassName(String className) {
    try {
      return Optional.of(
          (DatasetNameTrimmer) Class.forName(className).getDeclaredConstructor().newInstance());
    } catch (Exception e) {
      log.warn("Failed to initialize trimmer {}: {}", className, e.getMessage());
      return Optional.empty();
    }
  }
}
