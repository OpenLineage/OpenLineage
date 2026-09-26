/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a dynamically provisioned parameter whose value can be retrieved from system
 * properties or environment variables. Implementations define parameter names, prefixes, default
 * values, and logging.
 *
 * <p>Parameters can be specified with the {@code -D} JVM argument syntax {@code
 * -D<prefix>.<parameterName>=value}, or through an environment variable whose name is the full key
 * uppercased with dots and camelCase humps replaced by underscores.
 *
 * <p>For example, a prefix of {@code openlineage.tests.emr} and a parameter name of {@code
 * clusterId} can be set with {@code -Dopenlineage.tests.emr.clusterId=value} or {@code
 * OPENLINEAGE_TESTS_EMR_CLUSTER_ID=value}.
 *
 * <p>{@link #resolve()} retrieves the value from the system properties, falling back to the
 * environment variable and then to the default value, if provided.
 */
public interface DynamicParameter {
  // Hand-written stand-in for Lombok's @Slf4j, which can't be used on an interface: it emits a
  // private field, illegal here.
  Logger log = LoggerFactory.getLogger(DynamicParameter.class);

  /**
   * Returns the prefix applied to the parameter name when constructing the full system property key
   * (prefix + '.' + parameter name). May return {@code null} if no prefix is used.
   *
   * @return the prefix string or {@code null} if no prefix is used
   */
  String getPrefix();

  /**
   * Returns the parameter name appended to the prefix (if any) to form the complete key.
   *
   * @return the parameter name
   */
  String getParameterName();

  String name();

  /**
   * Returns the default value used when the parameter is not specified. May return {@code null} if
   * there is no default value.
   *
   * @return the default value or {@code null} if none is specified
   */
  String getDefaultValue();

  /**
   * The environment variable equivalent of the full system property key: uppercased, with dots and
   * camelCase humps replaced by underscores (e.g. {@code openlineage.tests.emr.clusterId} becomes
   * {@code OPENLINEAGE_TESTS_EMR_CLUSTER_ID}).
   *
   * @return the environment variable name
   */
  default String getEnvironmentVariableName() {
    String prefix = getPrefix() != null ? getPrefix() + "." : "";
    String key = prefix + getParameterName();
    return key.replaceAll("([a-z0-9])([A-Z])", "$1_$2").replace('.', '_').toUpperCase();
  }

  /**
   * Resolves the parameter value: from the system properties using the constructed key, else from
   * the corresponding environment variable, else the default value when provided.
   *
   * @return the resolved parameter value
   * @throws RuntimeException if the value is not found and no default value is provided
   */
  default String resolve() {
    String prefix = getPrefix() != null ? getPrefix() + "." : "";
    String key = prefix + getParameterName();
    log.debug("Resolving parameter [{}] using key [{}]", name(), key);
    String resolved = System.getProperty(key);
    if (resolved != null) {
      return resolved;
    }
    String envKey = getEnvironmentVariableName();
    resolved = System.getenv(envKey);
    if (resolved != null) {
      return resolved;
    }
    if (getDefaultValue() != null) {
      log.debug("Parameter [{}] not found; using the default value [{}]", key, getDefaultValue());
      return getDefaultValue();
    }
    throw new RuntimeException(
        "The value ["
            + key
            + "] could not be found in the system properties or the environment. Use `-D"
            + key
            + "=YOUR_VALUE` or `"
            + envKey
            + "=YOUR_VALUE`");
  }
}
