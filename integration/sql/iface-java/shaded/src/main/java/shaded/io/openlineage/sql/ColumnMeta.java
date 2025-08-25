/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package shaded.io.openlineage.sql;

import java.util.Optional;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Represents metadata for a database column, including its name and optional origin table.
 */
public class ColumnMeta {
  private final Optional<DbTableMeta> origin; // nullable
  private final String name;

  /**
   * Creates a new ColumnMeta instance.
   *
   * @param origin the table from which this column originates
   * @param name the name of the column
   */
  public ColumnMeta(DbTableMeta origin, String name) {
    this.origin = Optional.ofNullable(origin);
    this.name = name;
  }

  /**
   * Returns the optional origin table metadata for this column.
   *
   * @return the table metadata if known, empty Optional otherwise
   */
  public Optional<DbTableMeta> origin() {
    return origin;
  }

  /**
   * Returns the name of the column.
   *
   * @return the column name
   */
  public String name() {
    return name;
  }

  @Override
  public String toString() {
    return String.format(
        "{\"origin\": %s, \"name\": \"%s\"}",
        origin.isPresent() ? origin.get().toString() : "\"unknown\"", name);
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }

    if (!(o instanceof ColumnMeta)) {
      return false;
    }

    ColumnMeta other = (ColumnMeta) o;
    return other.origin().equals(origin()) && other.name.equals(name);
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(origin).append(name).toHashCode();
  }
}
