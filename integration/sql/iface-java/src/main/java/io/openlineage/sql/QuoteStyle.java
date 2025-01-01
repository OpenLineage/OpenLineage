/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.sql;

import org.apache.commons.lang3.builder.HashCodeBuilder;

public class QuoteStyle {
  private final String database;
  private final String schema;
  private final String name;

  public QuoteStyle(String database, String schema, String name) {
    this.database = database;
    this.schema = schema;
    this.name = name;
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
