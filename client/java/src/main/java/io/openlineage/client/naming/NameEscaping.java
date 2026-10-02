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
 * <p>The escaping rules (from the naming specification) are:
 *
 * <ol>
 *   <li>A literal {@code \} is replaced with {@code \\}.
 *   <li>A literal {@code .} is replaced with {@code \.}.
 * </ol>
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

  private static volatile Boolean configOverride;

  private NameEscaping() {}

  /**
   * Apply the {@code name.escaping} value loaded from configuration.
   *
   * <p>Call this when OpenLineage configuration has been loaded so that the {@code name.escaping}
   * setting is honoured globally. Pass {@code null} to reset to environment variable lookup.
   *
   * @param escaping {@code Boolean.TRUE} to enable escaping, {@code Boolean.FALSE} to disable it
   *     explicitly, or {@code null} to reset to env-var lookup.
   */
  public static void configure(Boolean escaping) {
    configOverride = escaping;
  }

  /**
   * Apply the {@link NameConfig} loaded from configuration.
   *
   * <p>Call this when OpenLineage configuration has been loaded so that the {@code name.escaping}
   * setting is honoured globally. Pass {@code null} or a config with null {@code escaping} to reset
   * to environment variable lookup.
   *
   * @param nameConfig the parsed name configuration, may be {@code null}
   */
  public static void configure(NameConfig nameConfig) {
    configOverride = nameConfig != null ? nameConfig.getEscaping() : null;
  }

  /**
   * Returns {@code true} if dot-escaping is enabled.
   *
   * <p>Resolution order:
   *
   * <ol>
   *   <li>If {@link #configure(Boolean)} or {@link #configure(NameConfig)} was called with a
   *       non-{@code null} escaping setting, that value is returned.
   *   <li>Otherwise the environment variable {@code OPENLINEAGE__NAME__ESCAPING} is consulted.
   * </ol>
   *
   * @return {@code true} when escaping is active
   */
  public static boolean isEscapingEnabled() {
    if (configOverride != null) {
      return configOverride;
    }
    return Boolean.valueOf(System.getenv(ENV_VAR));
  }

  /**
   * Returns {@code true} if dot-escaping is enabled, with the following resolution order:
   *
   * <ol>
   *   <li>If {@code nameConfig} is non-{@code null} and its {@code escaping} field is non-{@code
   *       null}, that value is returned.
   *   <li>Otherwise global configuration and the environment variable {@code
   *       OPENLINEAGE__NAME__ESCAPING} are consulted via {@link #isEscapingEnabled()}.
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
    return isEscapingEnabled() ? doEscape(segment) : segment;
  }

  /**
   * Escapes dots in a single name segment when escaping is enabled.
   *
   * <p>The transformation is applied in two steps so that backslashes already present in the
   * segment are not misinterpreted as escape sequences by consumers:
   *
   * <ol>
   *   <li>A literal {@code \} is replaced with {@code \\}.
   *   <li>A literal {@code .} is replaced with {@code \.}.
   * </ol>
   *
   * <p>This ensures that a segment such as {@code foo\.bar} (backslash followed by a dot) is
   * encoded as {@code foo\\\\.bar}, which a consumer can unambiguously decode back to the original.
   *
   * <p>The transformation is applied only when {@link #isEscapingEnabled(NameConfig)} returns
   * {@code true}; otherwise the segment is returned unchanged.
   *
   * @param segment a single name component (e.g. database, schema, table)
   * @param nameConfig the parsed name configuration, may be {@code null}
   * @return the segment with backslashes and dots escaped, or unchanged when escaping is disabled
   */
  public static String escapeSegment(String segment, NameConfig nameConfig) {
    return isEscapingEnabled(nameConfig) ? doEscape(segment) : segment;
  }

  private static String doEscape(String segment) {
    return segment.replace("\\", "\\\\").replace(".", "\\.");
  }
}
