/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.dataset.partition.trimmer;

public interface DatasetNameTrimmer {

  String SEPARATOR = "/";

  /**
   * Determines if a last path of a dataset can be trimmed.
   *
   * @param name
   * @return
   */
  boolean canTrim(String name);

  /**
   * Trims the last path of the dataset name if it can be trimmed.
   *
   * @param name
   * @return
   */
  default String trim(String name) {
    return canTrim(name) ? removeLastPath(name) : name;
  }

  /**
   * Returns the last path of the dataset name.
   *
   * @param name
   * @return
   */
  default String getLastPath(String name) {
    if (name == null || !name.contains(SEPARATOR)) {
      return name;
    }
    return name.substring(name.lastIndexOf(SEPARATOR)).replaceAll(SEPARATOR, "");
  }

  /**
   * /** Removes the last path from the dataset name.
   *
   * @param name dataset name
   * @return dataset name without the last path
   */
  static String removeLastPath(String name) {
    return name.substring(0, name.lastIndexOf(SEPARATOR));
  }
}
