/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import java.util.Locale;

/** Controls automatic translation between explicit lineage facets and legacy lineage fields. */
public enum LineageCompatibility {
  NONE,
  LEGACY,
  MODERN,
  BOTH;

  @JsonCreator
  public static LineageCompatibility fromString(String value) {
    return value == null ? null : valueOf(value.toUpperCase(Locale.ROOT));
  }
}
