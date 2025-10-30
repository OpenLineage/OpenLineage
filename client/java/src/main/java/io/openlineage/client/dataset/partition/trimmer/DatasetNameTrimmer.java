/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.dataset.partition.trimmer;

import org.apache.commons.lang3.StringUtils;

public interface DatasetNameTrimmer {

  String SEPARATOR = "/";

  /**
   * Determines if the last part of a dataset can be trimmed.
   *
   * @param name
   * @return
   */
  boolean canTrim(String name);

  /**
   * Trims the last part of the dataset name if it can be trimmed.
   *
   * @param name
   * @return
   */
  default String trim(String name) {
    return canTrim(name) ? removeLastPart(name) : name;
  }

  /**
   * Returns the last part of the dataset name.
   *
   * @param name
   * @return
   */
  default String getLastPart(String name) {
    if (name == null || !name.contains(SEPARATOR)) {
      return name;
    }
    String nameWithoutTrailingSlash = StringUtils.stripEnd(name, "/");
    return name.substring(nameWithoutTrailingSlash.lastIndexOf(SEPARATOR))
        .replaceAll(SEPARATOR, "");
  }

  /**
   * Checks if a name has more than one part when split by a slash separator.
   *
   * @param name
   * @return
   */
  default boolean hasMultipleDirectories(String name) {
    if (name == null || !name.contains(SEPARATOR)) {
      return false;
    }
    if (name.startsWith("/")) {
      return name.substring(1).split(SEPARATOR).length > 1;
    }
    return name.split(SEPARATOR).length > 1;
  }

  /**
   * /** Removes the last part from the dataset name.
   *
   * @param name dataset name
   * @return dataset name without the last part
   */
  static String removeLastPart(String name) {
    return name.substring(0, name.lastIndexOf(SEPARATOR));
  }
}
