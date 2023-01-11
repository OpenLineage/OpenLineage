/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.sql;

import java.util.Optional;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class ColumnMeta {
  private final Optional<DbTableMeta> origin; // nullable
  private final String name;

  public ColumnMeta(DbTableMeta origin, String name) {
    this.origin = Optional.ofNullable(origin);
    this.name = name;
  }

  public Optional<DbTableMeta> origin() {
    return origin;
  }

  public String name() {
    return name;
  }

  @Override
  public String toString() {
    return String.format(
        "{{\"origin\": %s, \"name\": %s}}", origin != null ? origin.toString() : "unknown", name);
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
