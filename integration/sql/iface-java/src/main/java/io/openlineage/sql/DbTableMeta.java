/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.sql;

import org.apache.commons.lang3.builder.HashCodeBuilder;

public class DbTableMeta {
  private final String database;
  private final String schema;
  private final String name;
  private final QuoteStyle quoteStyle;

  public DbTableMeta(String database, String schema, String name, QuoteStyle quoteStyle) {
    this.database = database;
    this.schema = schema;
    this.name = name;
    this.quoteStyle = quoteStyle;
  }

  public DbTableMeta(String database, String schema, String name) {
    this.database = database;
    this.schema = schema;
    this.name = name;
    this.quoteStyle = new QuoteStyle(null, null, null);
  }

  public String database() {
    return database;
  }

  public String schema() {
    return schema;
  }

  public String name() {
    return name;
  }

  public QuoteStyle quote_style() {
    return quoteStyle;
  }

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
