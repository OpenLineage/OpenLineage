/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.sql;

import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class ColumnLineage {
  private final ColumnMeta descendant;
  private final List<ColumnMeta> lineage;

  public ColumnLineage(ColumnMeta descendant, List<ColumnMeta> lineage) {
    this.descendant = descendant;
    this.lineage = lineage;
  }

  public ColumnMeta descendant() {
    return descendant;
  }

  public List<ColumnMeta> lineage() {
    return lineage;
  }

  @Override
  public String toString() {
    return String.format(
        "{{\"descendant\": %s, \"lineage\": %s}}",
        descendant.toString(), Arrays.toString(lineage.toArray()));
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }

    if (!(o instanceof ColumnLineage)) {
      return false;
    }

    ColumnLineage other = (ColumnLineage) o;
    return other.descendant.equals(descendant) && other.lineage.equals(lineage);
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(descendant).append(lineage).toHashCode();
  }
}
