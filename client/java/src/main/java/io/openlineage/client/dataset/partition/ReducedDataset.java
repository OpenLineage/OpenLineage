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

/**
 * Intermediate data structure to store datasets while being reduced.
 *
 * <p>Two datasets can be reduced if they have a common trimmed name and same facets. A logic used
 * to trim dataset name is defined in the {@link io.openlineage.client.dataset.DatasetConfig} via
 * collection of dataset trimmers. A default collection of trimmers can be altered with
 * extraTrimmers or disabledTrimmers settings.
 *
 * <p>A reduce operation returns a single dataset with a trimmed name of the reduced datasets and
 * all the facets of the reduced datasets. Additionally, a returned dataset is enriched with a
 * subset definition facet containing non-trimmed dataset names of all the datasets that were
 * reduced.
 *
 * <p>Reduce on a single dataset, with dataset name that can't be trimmed, results in an unmodified
 * dataset. Reduce on a single dataset, with dataset name that can be trimmed, returns a dataset
 * with a trimmed name and locations' based subset definition facet with a non-trimmed name of a
 * dataset.
 */
class ReducedDataset {

  @Getter private final String trimmedDatasetName;
  @Getter private final List<String> nonTrimmedNames;
  @Getter private final Dataset dataset;
  private final Collection<DatasetNameTrimmer> trimmers;

  static ReducedDataset of(DatasetConfig config, Dataset dataset) {
    return new ReducedDataset(config, dataset);
  }

  private ReducedDataset(DatasetConfig config, Dataset dataset) {
    this.trimmers = config.getDatasetNameTrimmers();
    this.trimmedDatasetName = trimDatasetName(dataset.getName());
    this.nonTrimmedNames = new ArrayList<>();
    if (!Objects.equals(trimmedDatasetName, dataset.getName())) {
      // reduced names are different from trimmed name -> add it
      this.nonTrimmedNames.add(dataset.getName());
    }
    this.dataset = dataset;
  }

  /**
   * Given another dataset, it reduces two datasets. If the trimmed name of the two datasets are
   * different, it returns an empty Optional. If the facets of the two datasets are different, it
   * returns an empty Optional. Otherwise, it returned a dataset with a trimmed name, common facets
   * and locations' based subset definition facet with all non-trimmed dataset names,
   *
   * @param other the other dataset
   * @return an empty Optional if the two datasets can't be reduced, the reduced dataset otherwise
   */
  public Optional<ReducedDataset> reduce(ReducedDataset other) {
    if (trimmedDatasetName == null || !trimmedDatasetName.equals(other.trimmedDatasetName)) {
      // null different names - can't be reduced
      return Optional.empty();
    }

    if (!hasSameFacets(other)) {
      return Optional.empty();
    }

    // non-trimmed names
    nonTrimmedNames.addAll(other.nonTrimmedNames);
    return Optional.of(this);
  }

  /**
   * Trims name based on config
   *
   * @return
   */
  private String trimDatasetName(String name) {
    String result = name;
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
