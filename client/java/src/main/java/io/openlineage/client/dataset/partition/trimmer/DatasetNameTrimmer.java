/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.dataset.partition.trimmer;

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
    return name.substring(name.lastIndexOf(SEPARATOR)).replaceAll(SEPARATOR, "");
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
