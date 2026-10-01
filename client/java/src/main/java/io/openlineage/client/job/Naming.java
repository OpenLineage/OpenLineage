/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.job;

import io.openlineage.client.naming.NameConfig;
import io.openlineage.client.naming.NameEscaping;
import javax.annotation.Nullable;
import lombok.Builder;

/**
 * Utility class for constructing job names according to the OpenLineage job naming conventions.
 *
 * <p>Supported job types include:
 *
 * <ul>
 *   <li>Spark: {@code {appName}.{command}.{table}}
 * </ul>
 */
@SuppressWarnings("PMD.MissingStaticMethodInNonInstantiatableClass")
public class Naming {

  private Naming() {}

  /** Interface representing a job name that can be resolved to a string. */
  public interface JobName {
    /**
     * Returns the formatted job name.
     *
     * @return a string representing the job name.
     */
    String getName();
  }

  /** Represents a Spark job name using the format: {@code {appName}.{command}.{table}}. */
  @Builder
  public static class Spark implements JobName {
    private final String appName;
    private final String command;
    private final String table;
    private final NameConfig nameConfig;

    /**
     * Constructs a new {@link Spark} job name.
     *
     * @param appName the Spark application name; must be non-null and non-empty
     * @param command the command or function being run
     * @param table the target table
     * @param nameConfig optional name configuration for dot-escaping
     * @throws IllegalArgumentException if appName is empty
     */
    public Spark(
        String appName,
        @Nullable String command,
        @Nullable String table,
        @Nullable NameConfig nameConfig) {
      if (appName.isEmpty()) {
        throw new IllegalArgumentException("appName, command, and table must be non-empty");
      }
      this.appName = appName;
      this.command = command;
      this.table = table;
      this.nameConfig = nameConfig;
    }

    public Spark(String appName, @Nullable String command, @Nullable String table) {
      this(appName, command, table, null);
    }

    /**
     * {@inheritDoc}
     *
     * @return the job name in the format: {@code {appName}.{command}.{table}}
     */
    @Override
    public String getName() {
      return NameEscaping.escapeSegment(appName, nameConfig)
          + (command != null ? "." + NameEscaping.escapeSegment(command, nameConfig) : "")
          + (table != null ? "." + NameEscaping.escapeSegment(table, nameConfig) : "");
    }
  }
}
