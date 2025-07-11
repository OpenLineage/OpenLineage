/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.job;

import javax.annotation.Nullable;
import lombok.Builder;

/**
 * Utility class for constructing job names according to the OpenLineage job naming conventions.
 *
 * <p>Supported job types include:
 *
 * <ul>
 *   <li>Spark: {@code {appName}.{command}.{table}}
 *   <li>Hive: {@code {operationName}}
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

    /**
     * Constructs a new {@link Spark} job name.
     *
     * @param appName the Spark application name; must be non-null and non-empty
     * @param command the command or function being run; must be non-null and non-empty
     * @param table the target table; must be non-null and non-empty
     * @throws IllegalArgumentException if any argument is empty
     * @throws NullPointerException if any argument is null
     */
    public Spark(String appName, @Nullable String command, @Nullable String table) {
      if (appName.isEmpty()) {
        throw new IllegalArgumentException("appName, command, and table must be non-empty");
      }
      this.appName = appName;
      this.command = command;
      this.table = table;
    }

    /**
     * {@inheritDoc}
     *
     * @return the job name in the format: {@code {appName}.{command}.{table}}
     */
    @Override
    public String getName() {
      return appName + (command != null ? "." + command : "") + (table != null ? "." + table : "");
    }
  }

  /** Represents a Hive job name */
  @Builder
  public static class Hive implements JobName {
    private final String operationName;

    public Hive(String operationName) {
      if (operationName == null || operationName.isEmpty()) {
        throw new IllegalArgumentException("operationName must be non-empty");
      }
      this.operationName = operationName;
    }

    @Override
    public String getName() {
      return operationName;
    }
  }
}
