/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.sql;

import org.apache.commons.lang3.builder.HashCodeBuilder;

public class ExtractionError {
  private final long index;
  private final String message;
  private final String originStatement;

  public ExtractionError(long index, String message, String originStatement) {
    this.index = index;
    this.message = message;
    this.originStatement = originStatement;
  }

  private long index() {
    return index;
  }

  public String message() {
    return message;
  }

  public String originStatement() {
    return originStatement;
  }

  @Override
  public String toString() {
    return String.format(
        "{{\"index\": %d, \"message\": %s, \"originStatement\": %s}}",
        index, message, originStatement);
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }

    if (!(o instanceof ExtractionError)) {
      return false;
    }

    ExtractionError other = (ExtractionError) o;
    return other.index() == index
        && other.message().equals(message)
        && other.originStatement().equals(originStatement);
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(index).append(message).append(originStatement).toHashCode();
  }
}
