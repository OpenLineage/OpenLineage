/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.sql;

import org.apache.commons.lang3.builder.HashCodeBuilder;

public class ColumnMeta {
  private final DbTableMeta origin; // nullable
  private final String name;

  public ColumnMeta(DbTableMeta origin, String name) {
    this.origin = origin;
    this.name = name;
  }

  public DbTableMeta origin() {
    return origin;
  }

  public String name() {
    return name;
  }

  @Override
  public String toString() {
    return String.format(
        "{{\"origin\": %s, \"name\": %s}}",
        origin != null ? origin.toString() : "unknown",
        name
    );
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
    return other.origin.equals(origin) && other.name.equals(name);
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(origin).append(name).toHashCode();
  }
}
