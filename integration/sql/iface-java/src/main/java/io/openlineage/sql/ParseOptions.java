/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.sql;

import java.util.Objects;

/**
 * Optional settings for {@link OpenLineageSql#parse(java.util.List, String, ParseOptions)}.
 *
 * <p>Use {@link #builder()} to create an instance. All settings are optional; unset values are
 * {@code null}.
 *
 * <p>{@code defaultDatabase} is the third (database) level of a {@code database.schema.table} name,
 * and {@code defaultSchema} is the second level. For dialects with only one namespace level above
 * the table (for example MySQL, Hive, or Spark without a catalog), the parser reads a two-part name
 * {@code x.t} as schema {@code x}, so callers should pass the connection's database or namespace as
 * {@code defaultSchema}, not {@code defaultDatabase}.
 */
public final class ParseOptions {
  private static final ParseOptions DEFAULTS = builder().build();

  private final String defaultSchema;
  private final String defaultDatabase;

  private ParseOptions(Builder builder) {
    this.defaultSchema = builder.defaultSchema;
    this.defaultDatabase = builder.defaultDatabase;
  }

  /**
   * Returns options with every setting unset.
   *
   * @return options with no defaults
   */
  public static ParseOptions defaults() {
    return DEFAULTS;
  }

  /**
   * Creates a new builder.
   *
   * @return a builder with every setting unset
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns the schema used for tables without an explicit schema.
   *
   * @return the default schema, or null
   */
  public String defaultSchema() {
    return defaultSchema;
  }

  /**
   * Returns the database used for tables without an explicit database.
   *
   * @return the default database, or null
   */
  public String defaultDatabase() {
    return defaultDatabase;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ParseOptions)) {
      return false;
    }
    ParseOptions other = (ParseOptions) o;
    return Objects.equals(defaultSchema, other.defaultSchema)
        && Objects.equals(defaultDatabase, other.defaultDatabase);
  }

  @Override
  public int hashCode() {
    return Objects.hash(defaultSchema, defaultDatabase);
  }

  @Override
  public String toString() {
    return String.format(
        "ParseOptions{defaultSchema=%s, defaultDatabase=%s}", defaultSchema, defaultDatabase);
  }

  /** Builder for {@link ParseOptions}. */
  public static final class Builder {
    private String defaultSchema;
    private String defaultDatabase;

    private Builder() {}

    /**
     * Sets the schema for tables without an explicit schema.
     *
     * @param defaultSchema the default schema, or null to leave it unspecified
     * @return this builder
     */
    public Builder defaultSchema(String defaultSchema) {
      this.defaultSchema = defaultSchema;
      return this;
    }

    /**
     * Sets the database for tables without an explicit database.
     *
     * @param defaultDatabase the default database, or null to leave it unspecified
     * @return this builder
     */
    public Builder defaultDatabase(String defaultDatabase) {
      this.defaultDatabase = defaultDatabase;
      return this;
    }

    /**
     * Builds an immutable {@link ParseOptions}.
     *
     * @return the options
     */
    public ParseOptions build() {
      return new ParseOptions(this);
    }
  }
}
