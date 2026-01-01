/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.sql;

import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Represents the quoting style for database identifiers (database, schema, table names).
 * Defines how identifiers should be quoted in SQL statements.
 */
public class QuoteStyle {
  private final String database;
  private final String schema;
  private final String name;

  /**
   * Creates a new QuoteStyle instance.
   *
   * @param database the quoting style for database names
   * @param schema the quoting style for schema names
   * @param name the quoting style for table/column names
   */
  public QuoteStyle(String database, String schema, String name) {
    this.database = database;
    this.schema = schema;
    this.name = name;
  }

  /**
   * Returns the database quoting style.
   *
   * @return the quoting style for database identifiers
   */
  public String database() {
    return database;
  }

  /**
   * Returns the schema quoting style.
   *
   * @return the quoting style for schema identifiers
   */
  public String schema() {
    return schema;
  }

  /**
   * Returns the name quoting style.
   *
   * @return the quoting style for table/column identifiers
   */
  public String name() {
    return name;
  }

  @Override
  public String toString() {
    return String.format(
        "{\"database\": %s, \"schema\": %s, \"name\": %s}", database, schema, name);
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(database).append(schema).append(name).toHashCode();
  }
}
