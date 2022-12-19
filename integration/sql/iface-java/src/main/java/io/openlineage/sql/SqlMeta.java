/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.sql;

import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class SqlMeta {
  private final List<DbTableMeta> inTables;
  private final List<DbTableMeta> outTables;
  private final List<ColumnLineage> columnLineage;

  public SqlMeta(List<DbTableMeta> in, List<DbTableMeta> out, List<ColumnLineage> columnLineage) {
    this.inTables = in;
    this.outTables = out;
    this.columnLineage = columnLineage;
  }

  public List<DbTableMeta> inTables() {
    return inTables;
  }

  public List<DbTableMeta> outTables() {
    return outTables;
  }

  public List<ColumnLineage> columnLineage() {
    return columnLineage;
  }

  @Override
  public String toString() {
    return String.format(
        "{{\"inTables\": %s, \"outTables\": %s, \"columnLineage\": %s}}",
        Arrays.toString(inTables.toArray()),
        Arrays.toString(outTables.toArray()),
        Arrays.toString(columnLineage.toArray()));
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }

    if (!(o instanceof SqlMeta)) {
      return false;
    }

    SqlMeta other = (SqlMeta) o;
    return other.inTables.equals(inTables)
        && other.outTables.equals(outTables)
        && other.columnLineage.equals(columnLineage);
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(inTables)
        .append(outTables)
        .append(columnLineage)
        .toHashCode();
  }
}
