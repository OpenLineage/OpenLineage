/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package shaded.io.openlineage.sql;

import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Contains metadata extracted from SQL statements, including table lineage,
 * column lineage, and any errors encountered during parsing.
 */
public class SqlMeta {
  private final List<DbTableMeta> inTables;
  private final List<DbTableMeta> outTables;
  private final List<ColumnLineage> columnLineage;
  private final List<ExtractionError> errors;

  /**
   * Creates a new SqlMeta instance.
   *
   * @param in list of input tables referenced in the SQL statements
   * @param out list of output tables created or modified by the SQL statements
   * @param columnLineage list of column lineage relationships
   * @param errors list of errors encountered during parsing
   */
  public SqlMeta(
      List<DbTableMeta> in,
      List<DbTableMeta> out,
      List<ColumnLineage> columnLineage,
      List<ExtractionError> errors) {
    this.inTables = in;
    this.outTables = out;
    this.columnLineage = columnLineage;
    this.errors = errors;
  }

  /**
   * Returns the list of input tables.
   *
   * @return tables that are read from or referenced in the SQL statements
   */
  public List<DbTableMeta> inTables() {
    return inTables;
  }

  /**
   * Returns the list of output tables.
   *
   * @return tables that are created, updated, or modified by the SQL statements
   */
  public List<DbTableMeta> outTables() {
    return outTables;
  }

  /**
   * Returns the column lineage information.
   *
   * @return list of column lineage relationships showing data flow between columns
   */
  public List<ColumnLineage> columnLineage() {
    return columnLineage;
  }

  /**
   * Returns any errors encountered during SQL parsing.
   *
   * @return list of extraction errors, empty if parsing was successful
   */
  public List<ExtractionError> errors() {
    return errors;
  }

  @Override
  public String toString() {
    return String.format(
        "{\"inTables\": %s, \"outTables\": %s, \"columnLineage\": %s, \"errors\": %s}",
        Arrays.toString(inTables.toArray()),
        Arrays.toString(outTables.toArray()),
        Arrays.toString(columnLineage.toArray()),
        Arrays.toString(errors.toArray()));
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
        && other.columnLineage.equals(columnLineage)
        && other.errors.equals(errors);
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(inTables)
        .append(outTables)
        .append(columnLineage)
        .append(errors)
        .toHashCode();
  }
}
