/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.client.dataset.partition;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.Dataset;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.dataset.DatasetConfig;
import io.openlineage.client.dataset.FacetUtils;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Given a list of datasets, class detects partitions and merges then into a single dataset with a
 * subset definition facet. Currently only path locations are supported.
 *
 * <p>If a path ends with key value directories, it is trimmed. If a path ends with directories with
 * a loose value detected, it is trimmed.
 *
 * <p>Trimming options are configurable within {@link io.openlineage.client.dataset.DatasetConfig}
 */
public class PartitionedDatasetReducer {

  private final DatasetConfig datasetConfig;
  private final OpenLineage openLineage;

  public PartitionedDatasetReducer(OpenLineage openLineage, DatasetConfig datasetConfig) {
    this.datasetConfig = datasetConfig;
    this.openLineage = openLineage;
  }

  /**
   * Given a list of input datasets, returns a new list of input datasets after applying partition
   * detection and merging rules.
   *
   * @param datasets list of input datasets
   * @return list of input datasets with partitions merged
   */
  public List<InputDataset> reduceInputs(List<InputDataset> datasets) {
    if (datasetConfig == null) {
      return datasets;
    }

    // sort inputs by dataset name
    datasets.sort(Comparator.comparing(InputDataset::getName));

    // reduce datasets
    List<ReducedDataset> reduced =
        reducedDatasets(datasets.stream().map(Dataset.class::cast).collect(Collectors.toList()));

    if (reduced.size() == datasets.size()) {
      // nothing to merge
      return datasets;
    }

    return reduced.stream()
        .map(
            r -> {
              InputDataset source = (InputDataset) r.getDataset();
              return openLineage
                  .newInputDatasetBuilder()
                  .name(r.getTrimmedPath())
                  .namespace(source.getNamespace())
                  .inputFacets(
                      FacetUtils.toBuilder(openLineage, source.getInputFacets())
                          // TODO: this generated code does not look well -> in case of oneOf gets
                          // first one to generate a code
                          .subset(
                              openLineage
                                  .newInputSubsetInputDatasetFacetBuilder()
                                  .inputCondition(
                                      openLineage
                                          .newLocationSubsetConditionBuilder()
                                          .locations(r.getReducedPaths())
                                          .build())
                                  .build())
                          .build())
                  .facets(source.getFacets())
                  .build();
            })
        .collect(Collectors.toList());
  }

  /**
   * Given a list of output datasets, returns a new list of output datasets after applying partition
   * detection and merging rules.
   *
   * @param datasets list of output datasets
   * @return list of output datasets with partitions merged
   */
  public List<OutputDataset> reduceOutputs(List<OutputDataset> datasets) {
    if (datasetConfig == null) {
      return datasets;
    }

    // sort inputs by dataset name
    datasets.sort(Comparator.comparing(Dataset::getName));

    // reduce datasets
    List<ReducedDataset> reduced =
        reducedDatasets(datasets.stream().map(Dataset.class::cast).collect(Collectors.toList()));

    if (reduced.size() == datasets.size()) {
      // nothing to merge
      return datasets;
    }

    return reduced.stream()
        .map(
            r -> {
              OutputDataset source = (OutputDataset) r.getDataset();
              return openLineage
                  .newOutputDatasetBuilder()
                  .name(r.getTrimmedPath())
                  .namespace(source.getNamespace())
                  .outputFacets(
                      FacetUtils.toBuilder(openLineage, source.getOutputFacets())
                          // TODO: this generated code does not look well -> in case of oneOf gets
                          // first one to generate a code
                          .put(
                              "subset",
                              openLineage
                                  .newOutputSubsetOutputDatasetFacetBuilder()
                                  .outputCondition(
                                      openLineage
                                          .newLocationSubsetConditionBuilder()
                                          .locations(r.getReducedPaths())
                                          .build())
                                  .build())
                          .build())
                  .facets(source.getFacets())
                  .build();
            })
        .collect(Collectors.toList());
  }

  private List<ReducedDataset> reducedDatasets(List<Dataset> datasets) {
    // reduce in a loop
    List<ReducedDataset> reducedDatasets = new ArrayList<>();
    ReducedDataset lastReducedDataset = null;
    for (Dataset dataset : datasets) {
      if (lastReducedDataset == null) {
        // first one -> nothing to reduce
        lastReducedDataset = ReducedDataset.of(datasetConfig, dataset);
        reducedDatasets.add(lastReducedDataset);
      } else {
        ReducedDataset reducedDataset = ReducedDataset.of(datasetConfig, dataset);
        Optional<ReducedDataset> reduced = lastReducedDataset.reduce(reducedDataset);
        if (!reduced.isPresent()) {
          // different paths
          lastReducedDataset = reducedDataset;
          reducedDatasets.add(lastReducedDataset);
        }
      }
    }
    return reducedDatasets;
  }
}
