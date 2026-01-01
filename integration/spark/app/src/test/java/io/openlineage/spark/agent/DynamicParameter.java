/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import javax.annotation.Nullable;
import org.slf4j.Logger;

/**
 * Represents a dynamically provisioned parameter whose value can be retrieved from system
 * properties. Implementations of this interface define parameter names, prefixes, default values,
 * and logging capabilities.
 *
 * <p>Parameters can be specified when running the application using the {@code -D} JVM argument
 * syntax: {@code -D<prefix>.<parameterName>=value}.
 *
 * <p>For example, if a parameter has a prefix of {@code openlineage.test} and a parameter name of
 * {@code clusterId}, it can be set using {@code -Dopenlineage.test.clusterId=value}.
 *
 * <p>The {@link #resolve()} method retrieves the parameter's value from the system properties,
 * returning the default value if the property is not set and a default is provided.
 */
public interface DynamicParameter {
  /**
   * Returns the prefix applied to the parameter name when constructing the full system property
   * key. The full key is formed by concatenating the prefix, a dot ('.'), and the parameter name.
   * If the prefix is not necessary, this method may return {@code null}.
   *
   * @return the prefix string or {@code null} if no prefix is used
   */
  @Nullable
  String getPrefix();

  /**
   * Returns the name of the parameter used when constructing the full system property key. This
   * name is appended to the prefix (if any) to form the complete key.
   *
   * @return the parameter name
   */
  String getParameterName();

  String name();

  /**
   * Returns the {@link Logger} instance used for logging messages.
   *
   * @return the Logger instance
   */
  Logger getLog();

  /**
   * Returns the default value of the parameter if it is not specified in the system properties. May
   * return {@code null} if there is no default value.
   *
   * @return the default value or {@code null} if none is specified
   */
  @Nullable
  String getDefaultValue();

  /**
   * Resolves the value of the parameter by retrieving it from the system properties using the
   * constructed key. If the parameter is not found in the system properties and a default value is
   * provided, the default value is returned. If the parameter is not found and no default value is
   * provided, a {@link RuntimeException} is thrown.
   *
   * @return the resolved parameter value
   * @throws RuntimeException if the parameter value is not found and no default value is provided
   */
  default String resolve() {
    // We can skip prefix in special cases where it is not used.
    String prefix = getPrefix() != null ? getPrefix() + "." : "";
    String key = prefix + getParameterName();
    getLog().debug("Resolving parameter [{}] using key [{}]", name(), key);
    String resolved = System.getProperty(key);
    if (resolved != null) {
      return resolved;
    } else {
      if (getDefaultValue() != null) {
        getLog()
            .debug(
                "The value for parameter [{}] has not been found. Using the default value [{}]",
                key,
                getDefaultValue());
        return getDefaultValue();
      }
    }
    throw new RuntimeException(
        "The value ["
            + key
            + "] could not be found in the system properties. Use `-D"
            + key
            + "=YOUR_VALUE`");
  }
}
