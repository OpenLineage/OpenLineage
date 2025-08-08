/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.udf.interfaces;

/**
 * An interface for retrieving additional lineage information from a Hive UDF.
 *
 * <p>Implementations of this interface provide additional metadata about the UDF, which can be
 * useful for lineage tracking or data governance purposes.
 */
public interface UDFAdditionalLineage {

  /**
   * Indicates whether the function obscures sensitive data.
   *
   * @return {@code true} if function is masking; {@code false} otherwise
   */
  Boolean isMasking();
}
