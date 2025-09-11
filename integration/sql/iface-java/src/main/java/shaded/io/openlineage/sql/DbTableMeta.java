/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package shaded.io.openlineage.sql;

import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Represents metadata for a database table.
 */
public class DbTableMeta {
  private final String database;
  private final String schema;
  private final String name;
  private final QuoteStyle quoteStyle;

  /**
   * Creates a new DbTableMeta instance with custom quote style.
   *
   * @param database the database name
   * @param schema the schema name
   * @param name the table name
   * @param quoteStyle the quoting style for identifiers
   */
  public DbTableMeta(String database, String schema, String name, QuoteStyle quoteStyle) {
    this.database = database;
    this.schema = schema;
    this.name = name;
    this.quoteStyle = quoteStyle;
  }

  /**
   * Creates a new DbTableMeta instance with default quote style.
   *
   * @param database the database name
   * @param schema the schema name
   * @param name the table name
   */
  public DbTableMeta(String database, String schema, String name) {
    this.database = database;
    this.schema = schema;
    this.name = name;
    this.quoteStyle = new QuoteStyle(null, null, null);
  }

  /**
   * Returns the database name.
   *
   * @return the database name
   */
  public String database() {
    return database;
  }

  /**
   * Returns the schema name.
   *
   * @return the schema name
   */
  public String schema() {
    return schema;
  }

  /**
   * Returns the table name.
   *
   * @return the table name
   */
  public String name() {
    return name;
  }

  /**
   * Returns the quote style for this table.
   *
   * @return the quote style configuration
   */
  public QuoteStyle quote_style() {
    return quoteStyle;
  }

  /**
   * Returns the fully qualified table name.
   *
   * @return the qualified name in the format database.schema.name
   */
  public String qualifiedName() {
    return String.format(
        "%s%s%s", database != null ? database + "." : "", schema != null ? schema + "." : "", name);
  }

  @Override
  public String toString() {
    return String.format(
        "\"%s%s%s\"",
        database != null ? database + "." : "", schema != null ? schema + "." : "", name);
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }

    if (!(o instanceof DbTableMeta)) {
      return false;
    }

    DbTableMeta other = (DbTableMeta) o;
    return qualifiedName().equals(other.qualifiedName());
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(database).append(schema).append(name).toHashCode();
  }
}
