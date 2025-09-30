/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.client.dataset.partition.trimmer;

/** Normalizes if last part is a string represents a key value pair in an arbitrary format. */
public class KeyValueTrimmer implements DatasetNameTrimmer {

  private static final char EQUALITY_SIGN = '=';

  @Override
  public boolean canTrim(String name) {
    String lastPart = getLastPart(name);

    return lastPart.indexOf(EQUALITY_SIGN) != -1
        && lastPart.indexOf(EQUALITY_SIGN) == lastPart.lastIndexOf(EQUALITY_SIGN);
  }
}
