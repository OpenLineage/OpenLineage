/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.dataset.partition;

import io.openlineage.client.OpenLineage.Dataset;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.dataset.DatasetConfig;
import java.nio.file.FileSystems;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;
import lombok.Getter;

class ReducedDataset {

  private static final String SEPARATOR = FileSystems.getDefault().getSeparator();
  private static final Pattern KEY_VALUE_PATTERN = Pattern.compile("(.*)=(.*)");

  @Getter private final String trimmedPath;
  @Getter private final List<String> reducedPaths;
  @Getter private final Dataset dataset;
  private final DatasetConfig config;

  static ReducedDataset of(DatasetConfig config, Dataset dataset) {
    return new ReducedDataset(config, dataset);
  }

  private ReducedDataset(DatasetConfig config, Dataset dataset) {
    this.config = config;
    this.trimmedPath = trimPath(dataset.getName());
    this.reducedPaths = new ArrayList<>();
    this.reducedPaths.add(dataset.getName());
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
    if (path == null || config == null) {
      return path;
    }
    boolean trimAtLooseDateDetected =
        Optional.ofNullable(config.getTrimAtLooseDateDetected()).orElse(false);
    boolean trimAtKeyValueDirs = Optional.ofNullable(config.getTrimAtKeyValueDirs()).orElse(false);

    List<String> dirs = Arrays.asList(path.split(SEPARATOR));
    List<String> trimmedDirs = new ArrayList<>();
    trimmedDirs.addAll(dirs);
    Collections.reverse(dirs); // reverse dirs

    for (String dir : dirs) {
      boolean dirTrimmed = false;
      if (trimAtLooseDateDetected && DateDetector.isDateMatch(dir)) {
        trimmedDirs.remove(trimmedDirs.size() - 1);
        dirTrimmed = true;
      }

      if (trimAtKeyValueDirs && KEY_VALUE_PATTERN.matcher(dir).matches()) {
        // key value detected - trim
        trimmedDirs.remove(trimmedDirs.size() - 1);
        dirTrimmed = true;
      }

      // nothing got trimmed - return what is left
      if (!dirTrimmed) {
        return String.join(SEPARATOR, trimmedDirs);
      }
    }

    // looks like trimmed everything, not intended -> return original
    return path;
  }

  private boolean hasSameFacets(ReducedDataset other) {
    // TODO: openlineage classes should implement equals and hashCode
    // Otherwise we need to compare JSONs serialized
    if (!areEqualSerialized(dataset.getFacets(), other.dataset.getFacets())) {
      return false;
    }

    if (dataset instanceof InputDataset) {
      // compare input facets
      InputDataset i1 = (InputDataset) dataset;
      InputDataset i2 = (InputDataset) other.dataset;
      if (!areEqualSerialized(i1.getInputFacets(), i2.getInputFacets())) {
        return false;
      }
    } else {
      // compare input facets
      OutputDataset o1 = (OutputDataset) dataset;
      OutputDataset o2 = (OutputDataset) other.dataset;
      if (!areEqualSerialized(o1.getOutputFacets(), o2.getOutputFacets())) {
        return false;
      }
    }

    return true;
  }

  private boolean areEqualSerialized(Object o1, Object o2) {
    return Objects.equals(OpenLineageClientUtils.toJson(o1), OpenLineageClientUtils.toJson(o2));
  }
}
