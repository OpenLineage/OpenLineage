/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.dataset.partition;

import io.openlineage.client.OpenLineage.Dataset;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.dataset.DatasetConfig;
import io.openlineage.client.dataset.partition.trimmer.DatasetNameTrimmer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import lombok.Getter;

class ReducedDataset {

  @Getter private final String trimmedPath;
  @Getter private final List<String> reducedPaths;
  @Getter private final Dataset dataset;
  private final Collection<DatasetNameTrimmer> trimmers;

  static ReducedDataset of(DatasetConfig config, Dataset dataset) {
    return new ReducedDataset(config, dataset);
  }

  private ReducedDataset(DatasetConfig config, Dataset dataset) {
    this.trimmers = config.getDatasetNameTrimmers();
    this.trimmedPath = trimPath(dataset.getName());
    this.reducedPaths = new ArrayList<>();
    if (!Objects.equals(trimmedPath, dataset.getName())) {
      // reduced paths are different from trimmed path -> add it
      this.reducedPaths.add(dataset.getName());
    }
    this.dataset = dataset;
  }

  /**
   * Given another dataset, it reduces and merges the two datasets. If the paths of the two datasets
   * are different, it returns an empty Optional. If the facets of the two datasets are different,
   * it returns an empty Optional. Otherwise, it merges the two datasets and returns the merged
   * dataset.
   *
   * @param other the other dataset
   * @return an empty Optional if the two datasets can't be merged, the merged dataset otherwise
   */
  public Optional<ReducedDataset> reduce(ReducedDataset other) {
    if (trimmedPath == null || !trimmedPath.equals(other.trimmedPath)) {
      // null different paths - can't be merged
      return Optional.empty();
    }

    if (!hasSameFacets(other)) {
      return Optional.empty();
    }

    // merge paths
    reducedPaths.addAll(other.reducedPaths);
    return Optional.of(this);
  }

  /**
   * Trims paths based on config
   *
   * @return
   */
  private String trimPath(String path) {
    String result = path;
    boolean continueTrimming = true;

    while (continueTrimming) {
      continueTrimming = false;
      for (DatasetNameTrimmer trimmer : trimmers) {
        if (trimmer.canTrim(result)) {
          result = trimmer.trim(result);
          continueTrimming = true;
        }
      }
    }

    // looks like trimmed everything, not intended -> return original
    return result;
  }

  private boolean hasSameFacets(ReducedDataset other) {
    if (!dataset.getFacets().equals(other.dataset.getFacets())) {
      return false;
    }

    if (dataset instanceof InputDataset) {
      // compare input facets
      InputDataset i1 = (InputDataset) dataset;
      InputDataset i2 = (InputDataset) other.dataset;
      return Optional.ofNullable(i1.getInputFacets())
          .equals(Optional.ofNullable(i2.getInputFacets()));
    } else {
      // compare output facets
      OutputDataset o1 = (OutputDataset) dataset;
      OutputDataset o2 = (OutputDataset) other.dataset;
      return Optional.ofNullable(o1.getOutputFacets())
          .equals(Optional.ofNullable(o2.getOutputFacets()));
    }
  }
}
