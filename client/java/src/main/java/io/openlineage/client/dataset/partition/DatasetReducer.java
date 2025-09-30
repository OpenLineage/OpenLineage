/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.client.dataset.partition;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.Dataset;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.InputDatasetInputFacetsBuilder;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.OutputDatasetOutputFacetsBuilder;
import io.openlineage.client.dataset.DatasetConfig;
import io.openlineage.client.dataset.FacetUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Class responsible for reducing datasets.
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
public class DatasetReducer {

  private final DatasetConfig datasetConfig;
  private final OpenLineage openLineage;

  public DatasetReducer(OpenLineage openLineage, DatasetConfig datasetConfig) {
    if (datasetConfig == null) {
      this.datasetConfig = new DatasetConfig(Collections.emptyMap(), null, null);
    } else this.datasetConfig = datasetConfig;
    this.openLineage = openLineage;
  }

  /**
   * Given a list of input datasets, returns a new list of input datasets after applying partition
   * detection and merging rules.
   *
   * @param datasets list of input datasets
   * @return list of reduced input datasets
   */
  public List<InputDataset> reduceInputs(List<InputDataset> datasets) {
    if (datasetConfig == null) {
      return datasets;
    }

    // reduce datasets
    List<ReducedDataset> reduced =
        reducedDatasets(datasets.stream().map(Dataset.class::cast).collect(Collectors.toList()));

    return reduced.stream()
        .map(
            r -> {
              InputDataset source = (InputDataset) r.getDataset();
              InputDatasetInputFacetsBuilder facetsBuilder =
                  FacetUtils.toBuilder(openLineage, source.getInputFacets());
              if (!r.getNonTrimmedNames().isEmpty()) {
                facetsBuilder.subset(
                    openLineage
                        .newInputSubsetInputDatasetFacetBuilder()
                        .inputCondition(
                            openLineage
                                .newLocationSubsetConditionBuilder()
                                .locations(r.getNonTrimmedNames())
                                .build())
                        .build());
              }
              return openLineage
                  .newInputDatasetBuilder()
                  .name(r.getTrimmedDatasetName())
                  .namespace(source.getNamespace())
                  .inputFacets(facetsBuilder.build())
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
   * @return list of reduced output datasets
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

    return reduced.stream()
        .map(
            r -> {
              OutputDataset source = (OutputDataset) r.getDataset();
              OutputDatasetOutputFacetsBuilder facetsBuilder =
                  FacetUtils.toBuilder(openLineage, source.getOutputFacets());
              if (!r.getNonTrimmedNames().isEmpty()) {
                facetsBuilder.put(
                    "subset",
                    openLineage
                        .newOutputSubsetOutputDatasetFacetBuilder()
                        .outputCondition(
                            openLineage
                                .newLocationSubsetConditionBuilder()
                                .locations(r.getNonTrimmedNames())
                                .build())
                        .build());
              }
              return openLineage
                  .newOutputDatasetBuilder()
                  .name(r.getTrimmedDatasetName())
                  .namespace(source.getNamespace())
                  .outputFacets(facetsBuilder.build())
                  .facets(source.getFacets())
                  .build();
            })
        .collect(Collectors.toList());
  }

  private List<ReducedDataset> reducedDatasets(List<Dataset> datasets) {
    Map<String, List<ReducedDataset>> toReduce =
        datasets.stream()
            .map(d -> ReducedDataset.of(datasetConfig, d))
            .collect(
                Collectors.toMap(
                    ReducedDataset::getTrimmedDatasetName,
                    d -> new ArrayList<>(Collections.singleton(d)),
                    (l1, l2) -> {
                      l1.addAll(l2);
                      return l1;
                    }));

    // Reducing logic is a bit more complex than just creating map based on trimmed name
    // If there are datasets with the same trimmed name but different facets, we can't reduce them
    // So, when having a list of dataset with the same trimmed name, we need to try reducing each
    // one with each other
    List<ReducedDataset> reducedDatasets = new ArrayList<>();
    for (List<ReducedDataset> sameNameList : toReduce.values()) {
      AtomicBoolean reducedSomething = new AtomicBoolean(true);

      // repeat as long as we reduce something
      while (reducedSomething.get()) {
        reducedSomething.set(false);
        ListIterator<ReducedDataset> iterator = sameNameList.listIterator();
        while (iterator.hasNext()) {
          ReducedDataset r1 = iterator.next();
          while (iterator.hasNext()) {
            ReducedDataset r2 = iterator.next();
            r1.reduce(r2)
                .ifPresent(
                    r -> {
                      iterator.remove();
                      reducedSomething.set(true);
                    });
          }
        }
      }
      reducedDatasets.addAll(sameNameList);
    }
    return reducedDatasets;
  }
}
