/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.naming;

import org.apache.commons.lang3.Strings;

/**
 * Utility class for escaping dots in OpenLineage name segments.
 *
 * <p>OpenLineage names are structured as dot-separated segments, e.g. {@code
 * {database}.{schema}.{table}}. When a segment itself contains a literal dot (e.g. an Oracle
 * service name {@code mydb.example.com}), the dot must be escaped so that consumers can
 * unambiguously split the name into its constituent parts.
 *
 * <p>The escaping rule (from the naming specification) is: a literal {@code .} inside a segment is
 * written as {@code \\.}.
 *
 * <p>Escaping is <em>enabled by default</em> and can be disabled by setting the environment
 * variable {@code OPENLINEAGE__NAME__ESCAPING} to {@code false} (case-insensitive). This is
 * intended as an escape hatch for producers that emit names that are already pre-escaped or that
 * target consumers which do not yet support the escaping convention.
 *
 * <p>Example:
 *
 * <pre>{@code
 * // "mydb\\.example\\.com.mySchema.myTable"
 * NameEscaping.escapeSegment("mydb.example.com") + "." + "mySchema" + "." + "myTable"
 * }</pre>
 */
public final class NameEscaping {

  private static final String ENV_VAR = "OPENLINEAGE__NAME__ESCAPING";

  private NameEscaping() {}

  /**
   * Returns {@code true} if dot-escaping is enabled (the default).
   *
   * <p>Escaping can be disabled by setting the environment variable {@code
   * OPENLINEAGE__NAME__ESCAPING=false} (case-insensitive).
   *
   * @return {@code true} when escaping is active
   */
  public static boolean isEscapingEnabled() {
    return !Strings.CI.equals("false", System.getenv(ENV_VAR));
  }

  /**
   * Escapes dots in a single name segment when escaping is enabled.
   *
   * <p>A literal {@code .} is replaced with {@code \\.} so that consumers can tell structural dots
   * (separating segments) from literal dots that are part of a segment value.
   *
   * <p>The transformation is applied only when {@link #isEscapingEnabled()} returns {@code true};
   * otherwise the segment is returned unchanged.
   *
   * @param segment a single name component (e.g. database, schema, table)
   * @return the segment with literal dots escaped, or unchanged when escaping is disabled
   */
  public static String escapeSegment(String segment) {
    return isEscapingEnabled() ? segment.replace(".", "\\.") : segment;
  }
}
