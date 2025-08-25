/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package shaded.io.openlineage.sql;

import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Represents the lineage of a column, tracking the relationship between a descendant column
 * and its source columns in SQL queries.
 */
public class ColumnLineage {
  private final ColumnMeta descendant;
  private final List<ColumnMeta> lineage;

  /**
   * Creates a new ColumnLineage instance.
   *
   * @param descendant the column that is derived from other columns
   * @param lineage the list of source columns that contribute to the descendant column
   */
  public ColumnLineage(ColumnMeta descendant, List<ColumnMeta> lineage) {
    this.descendant = descendant;
    this.lineage = lineage;
  }

  /**
   * Returns the descendant column metadata.
   *
   * @return the column that is derived from other columns
   */
  public ColumnMeta descendant() {
    return descendant;
  }

  /**
   * Returns the lineage of source columns.
   *
   * @return the list of source columns that contribute to the descendant column
   */
  public List<ColumnMeta> lineage() {
    return lineage;
  }

  @Override
  public String toString() {
    return String.format(
        "{\"descendant\": %s, \"lineage\": %s}",
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
