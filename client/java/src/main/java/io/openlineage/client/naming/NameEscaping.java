/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.naming;

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
 * <p>Escaping is <em>disabled by default</em> and can be enabled by setting the environment
 * variable {@code OPENLINEAGE__NAME__ESCAPING} to {@code true} (case-insensitive), or by setting
 * {@code name.escaping: true} in the YAML configuration.
 *
 * <p>When a {@link NameConfig} is available (e.g. loaded from YAML), prefer the overloads that
 * accept it — {@link #isEscapingEnabled(NameConfig)} and {@link #escapeSegment(String, NameConfig)}
 * — so that the YAML setting takes precedence over the environment variable. The zero-argument
 * overloads consult only the environment variable and are intended for call sites that do not have
 * access to a {@link NameConfig} instance.
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
   * Returns {@code true} if dot-escaping is enabled, consulting only the environment variable
   * {@code OPENLINEAGE__NAME__ESCAPING}.
   *
   * <p>Use {@link #isEscapingEnabled(NameConfig)} when a {@link NameConfig} is available so that
   * the YAML setting takes precedence.
   *
   * @return {@code true} when escaping is active
   */
  public static boolean isEscapingEnabled() {
    return Boolean.valueOf(System.getenv(ENV_VAR));
  }

  /**
   * Returns {@code true} if dot-escaping is enabled, with the following resolution order:
   *
   * <ol>
   *   <li>If {@code nameConfig} is non-{@code null} and its {@code escaping} field is non-{@code
   *       null}, that value is returned.
   *   <li>Otherwise the environment variable {@code OPENLINEAGE__NAME__ESCAPING} is consulted.
   * </ol>
   *
   * @param nameConfig the parsed name configuration, may be {@code null}
   * @return {@code true} when escaping is active
   */
  public static boolean isEscapingEnabled(NameConfig nameConfig) {
    if (nameConfig != null && nameConfig.getEscaping() != null) {
      return nameConfig.getEscaping();
    }
    return isEscapingEnabled();
  }

  /**
   * Escapes dots in a single name segment when escaping is enabled, consulting only the environment
   * variable.
   *
   * <p>Use {@link #escapeSegment(String, NameConfig)} when a {@link NameConfig} is available.
   *
   * @param segment a single name component (e.g. database, schema, table)
   * @return the segment with literal dots escaped, or unchanged when escaping is disabled
   */
  public static String escapeSegment(String segment) {
    return isEscapingEnabled() ? segment.replace(".", "\\.") : segment;
  }

  /**
   * Escapes dots in a single name segment when escaping is enabled.
   *
   * <p>A literal {@code .} is replaced with {@code \\.} so that consumers can tell structural dots
   * (separating segments) from literal dots that are part of a segment value.
   *
   * <p>The transformation is applied only when {@link #isEscapingEnabled(NameConfig)} returns
   * {@code true}; otherwise the segment is returned unchanged.
   *
   * @param segment a single name component (e.g. database, schema, table)
   * @param nameConfig the parsed name configuration, may be {@code null}
   * @return the segment with literal dots escaped, or unchanged when escaping is disabled
   */
  public static String escapeSegment(String segment, NameConfig nameConfig) {
    return isEscapingEnabled(nameConfig) ? segment.replace(".", "\\.") : segment;
  }
}
