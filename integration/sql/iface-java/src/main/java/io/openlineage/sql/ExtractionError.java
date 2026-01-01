/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.sql;

import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Represents an error that occurred during SQL parsing or lineage extraction.
 */
public class ExtractionError {
  private final long index;
  private final String message;
  private final String originStatement;

  /**
   * Creates a new ExtractionError instance.
   *
   * @param index the position or index where the error occurred
   * @param message the error message describing what went wrong
   * @param originStatement the original SQL statement that caused the error
   */
  public ExtractionError(long index, String message, String originStatement) {
    this.index = index;
    this.message = message;
    this.originStatement = originStatement;
  }

  private long index() {
    return index;
  }

  /**
   * Returns the error message.
   *
   * @return the error message describing what went wrong
   */
  public String message() {
    return message;
  }

  /**
   * Returns the original SQL statement that caused the error.
   *
   * @return the SQL statement where the error occurred
   */
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
